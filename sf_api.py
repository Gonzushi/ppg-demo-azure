from simple_salesforce import SalesforceLogin
from math import ceil
import pandas as pd
import aiohttp

class API():
    def __init__(self):
        self.api_version = 'v58.0'
        self.instance = 'https://abbottecho.my.salesforce.com'
        self._endpoint = self.instance + f'/services/data/{self.api_version}/'
        self.session_id = SalesforceLogin(username='hendry.widyanto@abbott.com.echo', password='Hw8751677!')[0]
        self.headers = {'Authorization': 'Bearer ' + self.session_id}
        self.query = '/services/data/v58.0/query/?q='

    async def query_field(self, object_name, field, conditions=[], session=aiohttp.ClientSession()):    
        SOQL = ''
        SOQL += f'SELECT {field} '
        SOQL += f'FROM {object_name} '
        if len(conditions) > 0: SOQL += 'WHERE ' + ' AND '.join(conditions) + ' '
        SOQL += f'GROUP BY {field} '
        SOQL.replace(' ', '+')

        async with session.get(self.instance + self.query + SOQL, headers=self.headers) as response:
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
        async with session.get(self.instance + self.query + SOQL, headers=self.headers) as response:
            data_json = await response.json()
        return data_json