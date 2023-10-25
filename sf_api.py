import aiohttp
import asyncio
import pandas as pd
import requests
import xml.dom.minidom
from collections import OrderedDict
from math import ceil

class Login():
    def __init__(self, username='', password='', session_id=''):
        self.api_version = 'v58.0'
        self.instance = 'https://abbottecho.my.salesforce.com'
        self.endpoint = self.instance + f'/services/data/{self.api_version}/'
        self.session_id = session_id
        self.login_succesful = True
        if not session_id: self._login(username, password)
    
    def check_session_id(self):
        headers = {'Authorization': 'Bearer ' + self.session_id}
        response = requests.get(self.endpoint, headers=headers)
        if response.status_code == 200: 
            self.login_succesful = True
        else:
            self.login_succesful = False
        
    def _login(self, username, password):
        client_id = 'simple-salesforce'
        soap_url = f'https://login.salesforce.com/services/Soap/u/{self.api_version}'
        request_body = f"""<?xml version="1.0" encoding="utf-8" ?>
                            <soapenv:Envelope
                                    xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
                                    xmlns:urn="urn:partner.soap.sforce.com">
                                <soapenv:Header>
                                    <urn:CallOptions>
                                        <urn:client>{client_id}</urn:client>
                                        <urn:defaultNamespace>sf</urn:defaultNamespace>
                                    </urn:CallOptions>
                                </soapenv:Header>
                                <soapenv:Body>
                                    <urn:login>
                                        <urn:username>{username}</urn:username>
                                        <urn:password>{password}</urn:password>
                                    </urn:login>
                                </soapenv:Body>
                            </soapenv:Envelope>"""
        request_headers = {'content-type': 'text/xml',
                           'charset': 'UTF-8',
                           'SOAPAction': 'login'}
        
        response = requests.post(soap_url, data=request_body, headers=request_headers)

        if response.status_code == 200: 
            self.login_succesful = True
            self.session_id = self._getUniqueElementValueFromXmlString(response.content, 'sessionId')
        else:
            self.login_succesful =False
            
    def _getUniqueElementValueFromXmlString(self, xmlString, elementName):
        """
        Extracts an element value from an XML string.

        For example, invoking
        getUniqueElementValueFromXmlString(
            '<?xml version="1.0" encoding="UTF-8"?><foo>bar</foo>', 'foo')
        should return the value 'bar'.
        """
        xmlStringAsDom = xml.dom.minidom.parseString(xmlString)
        elementsByName = xmlStringAsDom.getElementsByTagName(elementName)
        elementValue = None
        if len(elementsByName) > 0:
            elementValue = (
                elementsByName[0]
                .toxml()
                .replace('<' + elementName + '>', '')
                .replace('</' + elementName + '>', '')
            )
        return elementValue
    

class API(Login):
    def __init__(self, username='', password='', session_id=''):
        super().__init__(username, password, session_id)
        self.headers = {'Authorization': 'Bearer ' + self.session_id}
        if self.login_succesful: self.ext = OrderedDict(sorted(self._get_ext().items()))
        
    def _get_ext(self):
        response = requests.get(self.endpoint, headers=self.headers)
        return response.json() 
    
    def identity(self):
        response = requests.get(self.ext['identity'], headers=self.headers)
        return response.json() 

    async def query_field(self, object_name, field, conditions=[], session=aiohttp.ClientSession()):    
        SOQL = ''
        SOQL += f'SELECT {field} '
        SOQL += f'FROM {object_name} '
        if len(conditions) > 0: SOQL += 'WHERE ' + ' AND '.join(conditions) + ' '
        SOQL += f'GROUP BY {field} '
        SOQL.replace(' ', '+')

        async with session.get(self.instance + self.ext['query'] + '?q=' + SOQL, headers=self.headers) as response:
            data_json = await response.json()

        df = pd.DataFrame(data_json['records'])
        total_data = data_json['totalSize']
        data_size = len(df)
        total_read = ceil(total_data/data_size) - 1

        for i in range(total_read):
            async with session.get(self.instance + data_json['nextRecordsUrl'], headers=self.headers) as response:
                data_json = await response.json()
                df = pd.concat([df, pd.DataFrame(data_json['records'])])     

        df.drop('attributes', axis=1, inplace=True)
        df.sort_values(by=field, ignore_index=True, inplace=True)
        df.rename(columns={field: 'records'}, inplace=True)
        df.dropna(inplace=True)
        
        return df
    
    async def query_soql(self, SOQL, session=aiohttp.ClientSession()):
        async with session.get(self.instance + self.ext['query'] + '?q=' + SOQL, headers=self.headers) as response:
            data_json = await response.json()
        
        data = data_json['records']
        pages = ceil(data_json['totalSize']/2000)
        
        if pages > 1:
            url = data_json['nextRecordsUrl'][0:-4]

            links = [url + str(page*2000) for page in range(1, pages)]

            async def extra_query(link):
                async with session.get(self.instance + link, headers=self.headers) as response:
                    data_json = await response.json()
                    data.extend(data_json['records'])

            tasks = [extra_query(link) for link in links]
            await asyncio.gather(*tasks)
        
        return data
