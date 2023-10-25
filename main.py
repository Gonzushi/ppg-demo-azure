import aiohttp
import pandas as pd
import asyncio

from datetime import date
from fastapi import Cookie, FastAPI, Query, Depends, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from pydantic import BaseModel
from typing import Annotated
from sf_api import API, Login

import query_priority_list

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

app = FastAPI()

origins = [
    "http://localhost",
    "https://localhost",
    "http://localhost:5173",
    "https://localhost:5173",
    "http://localhost:4173",
    "http://localhost:4173",
    "https://abbottreport.netlify.app/",
    "http://abbottreport.netlify.app/",
    "*",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

df = pd.read_excel("_country_code.xlsx")
df = df[['Country Code', 'Country Name', 'EEA']].dropna().reset_index(drop=True)
EEA_country = df['Country Name'].to_list()

fields = {'product_segment': {'object': 'Product_Segment__c',
                              'field': 'Name',
                              'conditions': []},
          'rdc_code': {'object': 'CMPL123CME__Complaint_Code__c',
                       'field': 'Name',
                       'conditions': ["CMPL123CME__Type__c IN ('DEVICE CODE')"]},
          'rdc_clarifier': {'object': 'CMPL123CME__Complaint_Code__c',
                            'field': 'Clarifier__c',
                            'conditions': ["CMPL123CME__Type__c IN ('DEVICE CODE')"]},
          'pc_code': {'object': 'CMPL123CME__Complaint_Code__c',
                      'field': 'Name',
                      'conditions': ["CMPL123CME__Type__c IN ('HEALTH EFFECT - CLINICAL CODE', 'HEALTH EFFECT - IMPACT CODE')"]},
          'result_code': {'object': 'CMPL123CME__Complaint_Code__c',
                          'field': 'Name',
                          'conditions': ["CMPL123CME__Type__c IN ('EVAL RESULT CODE')"]},
          'conclusion_code': {'object': 'CMPL123CME__Complaint_Code__c',
                             'field': 'Name',
                             'conditions': ["CMPL123CME__Type__c IN ('EVAL CONCLUSION CODE')"]},
          'country': {'object': 'Country__c',
                      'field': 'Country_Name__c',
                      'conditions': []},
          'ears_product_family': {'object': 'CMPL123__Product__c',
                                  'field': 'EARS_Product_Family__c',
                                  'conditions': []},
         }

cache_field = {'n_product_segment': 0,
               'n_ears_product_family': 0,
               'n_rdc_code': 0,
               'n_rdc_clarifier': 0,
               'n_pc_code': 0,
               'n_result_code': 0,
               'n_conclusion_code': 0,
               'n_country': 0}


@app.on_event('startup')
async def startup_event():
    global session
    session = aiohttp.ClientSession()


@app.on_event('shutdown')
async def shutdown_event():
    await session.close()


@app.get('/')
async def home():
    return {'message': 'Hello Hendry!'}


@app.post('/token')
async def login(form_data: Annotated[OAuth2PasswordRequestForm, Depends()]):
    sf = API(username=form_data.username, password=form_data.password)
    if not sf.login_succesful:
        raise HTTPException(status_code=400, detail="Incorrect username or password")
    display_name = sf.identity()['display_name']

    content = {"access_token": sf.session_id, "token_type": "bearer", 'display_name': display_name}
    response = JSONResponse(content=content)
    response.set_cookie(key='session_id', value=sf.session_id)
    response.set_cookie(key='display_name', value=display_name)
    return response


@app.post('/verify_token')
async def verify_token(token: Annotated[str, Depends(oauth2_scheme)] = None):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Your sessions has timed out. Please log in again.",
        headers={"WWW-Authenticate": "Bearer"},
    )

    sf = Login(session_id=token)
    sf.check_session_id()
    if not sf.login_succesful: raise credentials_exception
    return {'detail': 'Session ID is valid'}


@app.get('/fields/{field_name}')
async def field(field_name: str | None = None,
                session_id: Annotated[str, Depends(oauth2_scheme)] = None):
    if cache_field['n_' + field_name] % 100 == 0:
        soql_component = fields[field_name]
        sf = API(session_id=session_id)
        data = await sf.query_field(soql_component['object'], soql_component['field'], soql_component['conditions'], session)
        data = data.to_dict('list')
        cache_field[field_name] = data
    else:
        data = cache_field[field_name]
    cache_field['n_' + field_name] = cache_field['n_' + field_name] + 1

    return data


@app.get('/eumir/')
async def eumir(start_date_of_event: date | None = None,
                end_date_of_event: date | None = None,
                product_segment: Annotated[list[str] | None, Query()] = None,
                rdc_code: Annotated[list[str] | None, Query()] = None,
                rdc_clarifier: Annotated[list[str] | None, Query()] = None,
                pc_code: Annotated[list[str] | None, Query()] = None,
                pc_severity: Annotated[list[str] | None, Query()] = None,
                result_code: Annotated[list[str] | None, Query()] = None,
                conclusion_code: Annotated[list[str] | None, Query()] = None,
                country_name: Annotated[list[str] | None, Query()] = None,
                complaint_code: Annotated[str | None, Query(pattern='(CN|Cn|cN|cn)-\d\d\d\d\d\d$')] = None,
                record_type: Annotated[str | None, Query()] = None,
                ears_product_family: Annotated[list[str] | None, Query()] = None,
                complaint_flag: Annotated[str | None, Query()] = None,
                reportable_flag: Annotated[str | None, Query()] = None,
                session_id: Annotated[str, Depends(oauth2_scheme)] = None):

    object = 'CMPL123CME__Complaint__c A'
    select_list = ['A.Id', 
                   'A.Name', 
                   'A.Product_Segment__c', 
                   'A.Date_of_Event__c', 
                   'A.Reportable_Country__c',
                   'A.CMPL123CME__CMPL123_WF_Status__c',]
    conditions = []
    if start_date_of_event: conditions.append('Date_of_Event__c >= {0}'.format(start_date_of_event))
    if end_date_of_event: conditions.append('Date_of_Event__c < {0}'.format(end_date_of_event))
    if product_segment: conditions.append("Product_Segment__c IN ('{0}')".format("', '".join(product_segment)))
    if ears_product_family: conditions.append("CMPL123CME__Product__r.EARS_Product_Family__c IN ('{0}')".format("', '".join(ears_product_family)))
    if complaint_code: conditions.append("Name NOT IN ('{0}')".format(complaint_code))

    if record_type:
        if record_type == 'Literature Search': conditions.append("Procedure__r.RecordTypeId IN ('0121R000001I5QYQA0')")
        if record_type == 'Trended': conditions.append("Procedure__r.RecordTypeId NOT IN ('0121R000001I5QYQA0')")

    if complaint_flag:
        if complaint_flag == 'No': 
            conditions.append("CMPL123CME__CMPL123_WF_Status__c IN ('Closed - No Complaint')")
            conditions.append("CMPL123CME__CMPL123_WF_Status__c NOT IN ('Closed - Void', 'Closed - Duplicate')")
        if complaint_flag == 'Yes': conditions.append("CMPL123CME__CMPL123_WF_Status__c NOT IN ('Closed - Void', 'Closed - Duplicate', 'Closed - No Complaint')")
    
    if complaint_flag == None: conditions.append("CMPL123CME__CMPL123_WF_Status__c NOT IN ('Closed - Void', 'Closed - Duplicate')")

    if reportable_flag:
        if reportable_flag == 'Yes': conditions.append("At_least_one_Reportable_is_not_CC__c IN ('Y')")
        if reportable_flag == 'No': conditions.append("At_least_one_Reportable_is_not_CC__c IN ('N')")

    child_filter_count = 0
    if rdc_code or rdc_clarifier: child_filter_count += 1
    if pc_code: child_filter_count += 1
    if result_code or conclusion_code: child_filter_count += 1

    temp_cond = []
    if pc_code: temp_cond.append("Code__c IN ('{0}')".format("', '".join(pc_code)))
    if pc_severity: temp_cond.append("Severity__c IN ({0})".format(", ".join(pc_severity)))
    if child_filter_count <= 2:
        if temp_cond: conditions.append("Id IN (SELECT Related_Complaint__c FROM Patient_Code__c WHERE {0})".format(' AND '.join(temp_cond)))
    else:
        if temp_cond: select_list.append("(SELECT Related_Complaint__c FROM Patient_Codes__r WHERE {0})".format(' AND '.join(temp_cond)))

    temp_cond = []
    if rdc_code: temp_cond.append("Code__c IN ('{0}')".format("', '".join(rdc_code)))
    if rdc_clarifier: temp_cond.append("Clarifier__c IN ('{0}')".format("', '".join(rdc_clarifier)))
    if temp_cond: conditions.append("Id IN (SELECT Related_Complaint__c FROM RDC_Code__c WHERE {0})".format(' AND '.join(temp_cond)))

    temp_cond = []
    if result_code: temp_cond.append("Evaluation_Result_Code__r.Name IN ('{0}')".format("', '".join(result_code)))
    if conclusion_code: temp_cond.append("Evaluation_Conclusion_Code__r.Name IN ('{0}')".format("', '".join(conclusion_code)))
    if temp_cond: conditions.append("Id IN (SELECT Related_Complaint__c FROM Engineering_Coding__c WHERE {0})".format(' AND '.join(temp_cond)))

    select_statement = ', '.join(select_list)
    conditions_statement = 'WHERE ' + ' AND '.join(conditions) if conditions else ''
    soql = 'SELECT {0} FROM {1} {2}'.format(select_statement, object, conditions_statement)
    soql = soql.strip()
    soql = soql.replace(' ', '+')

    sf = API(session_id=session_id)
    raw_data = await sf.query_soql(soql, session)
    df = pd.DataFrame(raw_data)
    df = df.dropna()
    df.reset_index(drop=True, inplace=True)
    if len(df) > 0:
        df.drop(columns="attributes", inplace=True)
        if child_filter_count > 2: df.drop(columns="Patient_Codes__r", inplace=True)

    if len(df) > 0:
        df_summary_table = pd.DataFrame()
        year_list = sorted(df['Date_of_Event__c'].str[:4].value_counts().index.to_list(), reverse=True)
        for year in year_list:
            df_complaint_year = df['Date_of_Event__c'].str[0:4] == year
            if country_name: df_summary_table.loc['Selected Country', year] = [country in country_name for country in df[df_complaint_year]['Reportable_Country__c']].count(True)
            df_summary_table.loc['EEA', year] = [country in EEA_country for country in df[df_complaint_year]['Reportable_Country__c']].count(True)
            df_summary_table.loc['World', year] = df_complaint_year.value_counts()[True]
        df_summary_table = df_summary_table.astype(int)

        summary_table = df_summary_table.rename_axis('#').reset_index().to_dict('records')
        summary_per_country = pd.DataFrame(df['Reportable_Country__c'].value_counts()).reset_index().to_dict('records')
    else:
        summary_table = []
        summary_per_country = []


    df = df[[country in country_name for country in df['Reportable_Country__c']]].reset_index(drop=True) if country_name and len(df) > 0 else df
    data = df.to_dict('records')

    response = {'data': data, 'summary_per_country': summary_per_country, 'summary_table': summary_table}

    return response


@app.get('/priority_list/{type}')
async def eumir(type: str,
                session_id: Annotated[str, Depends(oauth2_scheme)] = None):
    sf = API(session_id=session_id)
    df = query_priority_list.query(sf, option=type)
    response = df.to_dict('records')
    return response

@app.get('/article/')
async def eumir(start_date_of_event: date | None = None,
                end_date_of_event: date | None = None,
                product_segment: Annotated[list[str] | None, Query()] = None,
                rdc_code: Annotated[list[str] | None, Query()] = None,
                pc_code: Annotated[list[str] | None, Query()] = None,
                country_name: Annotated[list[str] | None, Query()] = None,
                complaint_code: Annotated[str | None, Query(pattern='(CN|Cn|cN|cn)-\d\d\d\d\d\d$')] = None,
                record_type: Annotated[str | None, Query()] = None,
                ears_product_family: Annotated[list[str] | None, Query()] = None,
                complaint_flag: Annotated[str | None, Query()] = None,
                reportable_flag: Annotated[str | None, Query()] = None,
                session_id: Annotated[str, Depends(oauth2_scheme)] = None):

    object = 'CMPL123CME__Complaint__c A'
    select_list = ['A.Id', 
                   'A.Name', 
                   'A.Product_Segment__c', 
                   'A.Date_of_Event__c', 
                   'A.Reportable_Country__c',
                   'A.CMPL123CME__CMPL123_WF_Status__c',]
    conditions = []
    if start_date_of_event: conditions.append('Date_of_Event__c >= {0}'.format(start_date_of_event))
    if end_date_of_event: conditions.append('Date_of_Event__c < {0}'.format(end_date_of_event))
    if product_segment: conditions.append("Product_Segment__c IN ('{0}')".format("', '".join(product_segment)))
    if ears_product_family: conditions.append("CMPL123CME__Product__r.EARS_Product_Family__c IN ('{0}')".format("', '".join(ears_product_family)))
    if complaint_code: conditions.append("Name NOT IN ('{0}')".format(complaint_code))

    if record_type:
        if record_type == 'Literature Search': conditions.append("Procedure__r.RecordTypeId IN ('0121R000001I5QYQA0')")
        if record_type == 'Trended': conditions.append("Procedure__r.RecordTypeId NOT IN ('0121R000001I5QYQA0')")

    if complaint_flag:
        if complaint_flag == 'No': 
            conditions.append("CMPL123CME__CMPL123_WF_Status__c IN ('Closed - No Complaint')")
            conditions.append("CMPL123CME__CMPL123_WF_Status__c NOT IN ('Closed - Void', 'Closed - Duplicate')")
        if complaint_flag == 'Yes': conditions.append("CMPL123CME__CMPL123_WF_Status__c NOT IN ('Closed - Void', 'Closed - Duplicate', 'Closed - No Complaint')")
    
    if complaint_flag == None: conditions.append("CMPL123CME__CMPL123_WF_Status__c NOT IN ('Closed - Void', 'Closed - Duplicate')")

    if reportable_flag:
        if reportable_flag == 'Yes': conditions.append("At_least_one_Reportable_is_not_CC__c IN ('Y')")
        if reportable_flag == 'No': conditions.append("At_least_one_Reportable_is_not_CC__c IN ('N')")


    sf = API(session_id=session_id)
    select_statement = ', '.join(select_list)
    year_list = [year for year in range(int(start_date_of_event.year), int(end_date_of_event.year)+1)]
    year_list.sort()
    year_list.reverse()
        
    async def query_code(rdc, object_code, code_type):
        conditions_code = conditions.copy()
        conditions_code.append("Id IN (SELECT Related_Complaint__c FROM {0} WHERE Code__c IN ('{1}'))".format(object_code, rdc))
        conditions_statement_code = 'WHERE ' + ' AND '.join(conditions_code) if conditions_code else ''
        soql_code = 'SELECT {0} FROM {1} {2}'.format(select_statement, object, conditions_statement_code)
        soql_code = soql_code.strip()
        soql_code = soql_code.replace(' ', '+')
        raw_data_code = await sf.query_soql(soql_code, session)
        df_code = pd.DataFrame(raw_data_code)
        if len(df_code) != 0: 
            df_code.reset_index(drop=True, inplace=True)
            df_code.drop(columns="attributes", inplace=True)
            df_code['Year'] = df_code['Date_of_Event__c'].str[0:4].astype(int)
            df_code['EEA'] = df_code['Reportable_Country__c'].map(lambda row: 'Yes' if row in EEA_country else 'No')
            if country_name:
                df_code['Selected'] = df_code['Reportable_Country__c'].map(lambda row: 'Yes' if row in country_name else 'No')
                
            df_output = pd.DataFrame()  
            for year in year_list:
                df_output.loc[f'{code_type} {rdc}', year] = year
                if country_name:
                    df_output.loc['Selected Country', year] = len(df_code[(df_code['Selected'] == 'Yes') & (df_code['Year'] == year)])
                df_output.loc['EEA', year] = len(df_code[(df_code['EEA'] == 'Yes') & (df_code['Year'] == year)])
                df_output.loc['World', year] = len(df_code[(df_code['Year'] == year)])
        else:
            df_output = pd.DataFrame()  
            for year in year_list:
                df_output.loc[f'{code_type} {rdc}', year] = year
                if country_name:
                    df_output.loc['Selected Country', year] = 0
                df_output.loc['EEA', year] = 0
                df_output.loc['World', year] = 0

        df_output = df_output.astype(int)
        df_output = df_output.reset_index()
        return df_output

    if rdc_code:
        tasks = [query_code(rdc, 'Rdc_Code__c', 'RDC') for rdc in rdc_code]
        results = await asyncio.gather(*tasks)
        df_rdc = pd.concat(results)
        df_rdc.reset_index(drop=True, inplace=True)

    if pc_code:
        tasks = [query_code(pc, 'Patient_Code__c', 'PC') for pc in pc_code]
        results = await asyncio.gather(*tasks)
        df_pc = pd.concat(results)
        df_pc.reset_index(drop=True, inplace=True)

    if rdc_code and pc_code:
        df_final = pd.concat([df_rdc, df_pc])
    elif rdc_code:
        df_final = df_rdc
    elif pc_code:
        df_final = df_pc
    
    df_final = df_final.rename(columns={'index': '#'})
    response = df_final.to_dict('records')
    
    return response


