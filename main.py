import aiohttp
import pandas as pd

from datetime import date
from fastapi import Cookie, FastAPI, Query, Depends, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from pydantic import BaseModel
from typing import Annotated
from sf_api import API, Login

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
                      'conditions': []}
         }

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
    soql_component = fields[field_name]
    sf = API(session_id=session_id)
    data = await sf.query_field(soql_component['object'], soql_component['field'], soql_component['conditions'], session)
    data = data.to_dict('list')
    return data


@app.get('/eumir/')
async def eumir(start_date_of_event: date,
                end_date_of_event: date,
                product_segment: Annotated[list[str], Query()],
                rdc_code: Annotated[list[str] | None, Query()] = None,
                rdc_clarifier: Annotated[list[str] | None, Query()] = None,
                pc_code: Annotated[list[str] | None, Query()] = None,
                result_code: Annotated[list[str] | None, Query()] = None,
                conclusion_code: Annotated[list[str] | None, Query()] = None,
                country_name: Annotated[list[str] | None, Query()] = None,
                complaint_code: Annotated[str | None, Query(pattern='CN-\d\d\d\d\d\d$')] = None,
                session_id: Annotated[str, Depends(oauth2_scheme)] = None):

    object = 'CMPL123CME__Complaint__c A'
    select_list = ['A.Id', 
                   'A.Name', 
                   'A.Product_Segment__c', 
                   'A.Date_of_Event__c', 
                   'A.Reportable_Country__c']
    conditions = []
    if start_date_of_event: conditions.append('Date_of_Event__c >= {0}'.format(start_date_of_event))
    if end_date_of_event: conditions.append('Date_of_Event__c < {0}'.format(end_date_of_event))
    if product_segment: conditions.append("Product_Segment__c IN ('{0}')".format("', '".join(product_segment)))
    if country_name: conditions.append("Reportable_Country__c IN ('{0}')".format("', '".join(country_name)))
    if complaint_code: conditions.append("Name NOT IN ('{0}')".format(complaint_code))

    child_filter_count = 0
    if rdc_code or rdc_clarifier: child_filter_count += 1
    if pc_code: child_filter_count += 1
    if result_code or conclusion_code: child_filter_count += 1

    if child_filter_count <= 2:
        if pc_code: conditions.append("Id IN (SELECT Related_Complaint__c FROM Patient_Code__c WHERE Code__c IN ('{0}'))".format("', '".join(pc_code)))
    else:
        if pc_code: select_list.append("(SELECT Related_Complaint__c FROM Patient_Codes__r WHERE Code__c IN ('{0}'))".format("', '".join(pc_code)))

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
    data = await sf.query_soql(soql, session)
    data = pd.DataFrame(data)
    data = data.dropna()
    if len(data) > 0:
        data.drop(columns="attributes", inplace=True)
        if child_filter_count > 2: data.drop(columns="Patient_Codes__r", inplace=True)
    data = data.to_dict('records')

    return data





