from simple_salesforce import SalesforceLogin
from collections import OrderedDict
from pathlib import Path
from selenium import webdriver

import pandas as pd

import html
import re
import requests
import webbrowser

from math import ceil

class Login():
    def __init__(self):
        self.api_version = 'v58.0'
        self.instance = 'https://abbottecho.my.salesforce.com'
        self._endpoint = self.instance + f'/services/data/{self.api_version}/'
        self._file_session_id = '_session_id.txt'
        self.session_id = self._check_session_id()
        
    def dev_guide(self):
        link = 'https://developer.salesforce.com/docs/atlas.en-us.244.0.api_rest.meta/api_rest/intro_rest.htm'
        webbrowser.open(link)
        
    def _check_session_id(self):
        session_id = self._read_session_id()
        r = requests.get(self._endpoint, headers={'Authorization': 'Bearer ' + session_id})
        if r.status_code != 200:
            session_id = self._get_session_id_using_email()
        return session_id
    
    def _read_session_id(self):
        if Path(self._file_session_id).is_file() == True:
            with open (self._file_session_id, 'r') as file:  
                session_id = file.readline()
        else:
            session_id = self._get_session_id_using_email()
        return session_id

    def _get_session_id_using_email(self):
        session_id, instance = SalesforceLogin(username='hendry.widyanto@abbott.com.echo', password='Hw8751677!')
        with open(self._file_session_id, 'w') as file:  
            file.write(session_id)
        return session_id
      
    def _get_saml_link(self):
        pattern = re.compile('https://abbottecho.my.salesforce.com/saml/[a-zA-Z0-9!@#$%^&*()_+-={}:;<>,.?/]*')
        html_doc = requests.get(self.instance).text
        html_doc = html.unescape(html_doc)
        saml_link = re.findall(pattern, html_doc)[0]
        return saml_link
        
    def _get_session_id(self):

        saml_link = self._get_saml_link()
        options = webdriver.EdgeOptions()
        options.add_argument('--headless')
        driver = webdriver.Edge(options=options)
        driver.implicitly_wait(10)
        driver.get(saml_link)
        
        for i in range(0, 2000):
            cookies = [cookie for cookie in driver.get_cookies() if 'sid' in cookie.values()]
            if len(cookies) == 1: break
        
        session_id = cookies[0]['value']
        driver.close()
        
        with open(self._file_session_id, 'w') as file:  
            file.write(session_id)
            
        return session_id
    
class API(Login):
    def __init__(self):
        super().__init__()
        self.headers = {'Authorization': 'Bearer ' + self.session_id}
        self.ext = OrderedDict(sorted(self._get_ext().items()))
        self.sobjects = self._sobjects()
        
    def _get_ext(self):
        r = requests.get(self._endpoint, headers=self.headers)
        return r.json()   
    
    def _sobjects(self):
        r = requests.get(self.instance + self.ext['sobjects'], headers=self.headers)
        data = r.json()['sobjects']
        df = pd.DataFrame(data)
        df = df[['label', 'name']].sort_values(by='label').reset_index(drop=True)
        return df

    def sobjects_search(self, text):
        return self.sobjects[self.sobjects['label'].str.lower().str.contains(text.lower())]
    
    def object_field(self, object_name, search=''):
        r = requests.get(self.instance + self.ext['sobjects'] + f'/{object_name}/describe', headers=self.headers)
        data = r.json()['fields']
        df = pd.DataFrame(data)
        df = df[['label', 'name', 'type', 'length', 'updateable']]
        df = df.sort_values(by='label').reset_index(drop=True)
        
        if search != '':
            df = df[df['label'].str.lower().str.contains(search.lower())]
            
        return df
    
    def query(self, object_name, fields_with_rename={}, conditions=[], more=1, rename=True):
        if fields_with_rename == {}:
            df = self.object_field(object_name)
            fields_with_rename = df[['label', 'name']].set_index('name').to_dict()['label']
        fields = [name for name in fields_with_rename.keys()]
        
        SOQL = ''
        SOQL += 'SELECT ' + ', '.join(fields) + ' '
        SOQL += f'FROM {object_name} '
        if len(conditions) > 0: SOQL += 'WHERE ' + ' AND '.join(conditions) + ' '
        
        r = requests.get(self.instance + self.ext['query'] + '?q=' + SOQL.replace(' ', '+'), headers=self.headers)
        data_json = r.json()
        df = pd.DataFrame(data_json['records'])
        total_data = data_json['totalSize']
        data_size = len(df)
        
        if more == 0 or more > total_data/data_size:
            total_read = ceil(total_data/data_size) - 1
        elif more == 1:
            total_read = 0
        else:
            total_read = more - 1
            
        for i in range(total_read):
            r = requests.get(self.instance + data_json['nextRecordsUrl'], headers=self.headers)
            data_json = r.json()
            df = pd.concat([df, pd.DataFrame(data_json['records'])])
            
        df.drop('attributes', axis=1, inplace=True)
        if rename == True: 
            df = df.rename(columns=fields_with_rename)
        
        return df
    
    def query_ori(self, object_name, field, conditions=[]):    
        SOQL = ''
        SOQL += f'SELECT {field} '
        SOQL += f'FROM {object_name} '
        if len(conditions) > 0: SOQL += 'WHERE ' + ' AND '.join(conditions) + ' '
        SOQL += f'GROUP BY {field} '
        
        r = requests.get(self.instance + self.ext['query'] + '?q=' + SOQL.replace(' ', '+'), headers=self.headers)
        data_json = r.json()
        df = pd.DataFrame(data_json['records'])
        total_data = data_json['totalSize']
        data_size = len(df)
        total_read = ceil(total_data/data_size) - 1
            
        for i in range(total_read):
            r = requests.get(self.instance + data_json['nextRecordsUrl'], headers=self.headers)
            data_json = r.json()
            df = pd.concat([df, pd.DataFrame(data_json['records'])])
            
        df.drop('attributes', axis=1, inplace=True)
        df.sort_values(by=field, ignore_index=True, inplace=True)
        df.rename(columns={field: 'records'}, inplace=True)
        df.dropna(inplace=True)
        
        return df