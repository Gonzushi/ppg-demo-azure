from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from typing import Annotated
from pydantic import BaseModel
from datetime import date
from sf_api import API
import requests
import pandas as pd

import asyncio
import aiohttp

from math import ceil


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

@app.get('/fields/{field_name}')
async def field(field_name: str | None = None):
    soql_component = fields[field_name]
    sf = API()
    data = await sf.query_field(soql_component['object'], soql_component['field'], soql_component['conditions'], session)
    data = data.to_dict('list')
    return data

@app.get('/eumir/')
async def eumir(start_date_of_event: date | None = None,
                end_date_of_event: date | None = None,
                product_segment: Annotated[list[str] | None, Query()] = None,
                rdc_code: Annotated[list[str] | None, Query()] = None,
                rdc_clarifier: Annotated[list[str] | None, Query()] = None,
                pc_code: Annotated[list[str] | None, Query()] = None,
                result_code: Annotated[list[str] | None, Query()] = None,
                conclusion_code: Annotated[list[str] | None, Query()] = None,
                country_name: Annotated[list[str] | None, Query()] = None,
                complaint_code: Annotated[str | None, Query(pattern='CN-\d\d\d\d\d\d$')] = None):
    
    object = 'CMPL123CME__Complaint__c A'
    select_list = ['A.Id', 
                   'A.Name', 
                   'A.Product_Segment__c', 
                   'A.Date_of_Event__c', 
                   'A.Reportable_Country__c']
    
    temp_cond = []
    if rdc_code: temp_cond.append("Code__c IN ('{0}')".format("', '".join(rdc_code)))
    if rdc_clarifier: temp_cond.append("Clarifier__c IN ('{0}')".format("', '".join(rdc_clarifier)))
    if temp_cond: select_list.append("(SELECT Id FROM RDC_Codes__r WHERE {0})".format(' AND '.join(temp_cond)))
    
    if pc_code: select_list.append("(SELECT Id FROM Patient_Codes__r WHERE Code__c IN ('{0}'))".format("', '".join(pc_code)))
    
    temp_cond = []
    if result_code: temp_cond.append("Evaluation_Result_Code__r.Name IN ('{0}')".format("', '".join(result_code)))
    if conclusion_code: temp_cond.append("Evaluation_Conclusion_Code__r.Name IN ('{0}')".format("', '".join(conclusion_code)))
    if temp_cond: select_list.append("(SELECT Id FROM Engineering_Codings__r WHERE {0})".format(' AND '.join(temp_cond)))
    
    conditions = []
    if start_date_of_event: conditions.append('Date_of_Event__c > {0}'.format(start_date_of_event))
    if end_date_of_event: conditions.append('Date_of_Event__c < {0}'.format(end_date_of_event))
    if product_segment: conditions.append("Product_Segment__c IN ('{0}')".format("', '".join(product_segment)))
    if country_name: conditions.append("Reportable_Country__c IN ('{0}')".format("', '".join(country_name)))
    if complaint_code: conditions.append("Name NOT IN ('{0}')".format(complaint_code))

    select_statement = ', '.join(select_list)
    conditions_statement = 'WHERE ' + ' AND '.join(conditions) if conditions else ''
    soql = 'SELECT {0} FROM {1} {2}'.format(select_statement, object, conditions_statement)
    soql = soql.strip()
    soql = soql.replace(' ', '+')

    sf = API()
    data = await sf.query_soql(soql, session)
    data = pd.DataFrame(data['records'])
    data = data.dropna()
    data = data.to_dict('records')
    return data