import calendar
import pandas as pd
import requests
from datetime import datetime

def combine_child_data(datas, columns=[], separator='\n'):
    text_output_list = []
    if datas == None: return ''
    datas = datas['records']
    for m in range(len(datas)):
        data = datas[m]
        data_check = []
        for n in range(len(columns)):
            column_name = columns[n]
            data_check.append(data[column_name] if data[column_name] != None else 'N/A')
        text_output_list.append(' - '.join(data_check))
        text_output = separator.join(sorted(set(text_output_list)))
    return text_output

def assign_priority(age_of_complaint, days_in_inv_queue, age_EOM):
    if age_of_complaint >= 60: return 'Priority 1 (> 60 days old now)'
    if days_in_inv_queue > 12: return 'Priority 2 (>12 days in queue)'
    if age_EOM >= 60: return 'Priority 3 (> 60 days old at EOM)'
    return 'Priority 4'

def assign_bucket(category):
    if category == 'FFR': return 'OCT/FFR'
    if category == 'OCT': return 'OCT/FFR'
    if category == 'Coroventis': return 'OCT/FFR'
    if category == 'VC': return 'VC'
    if category == 'EPS': return 'EPS'
    if category == 'GW': return 'GW'
    if category == 'DIL': return 'DIL'
    if category == 'SES': return 'SES'
    return 'Core'
    
def query(sf, option='queue'):
    date_now = datetime.now()
    last_day = calendar.monthrange(date_now.year, date_now.month)[1]
    date_EOM = datetime.strptime(f'{date_now.year}-{date_now.month}-{last_day}', "%Y-%m-%d")

    SOQL = "SELECT A.Id\
                , A.Name\
                , A.CMPL123CME__Product__r.Product_Category_Formula__c\
                , A.Product_Segment__c\
                , A.Reportable_Country__c\
                , A.CMPL123CME__Product__r.Name\
                , (SELECT OwnerId, Id, Name, CMPL123_WF_Status__c FROM CMPL123CME__Investigations__r)\
                , A.Alert_Date__c\
                , A.Investigation_Done_On__c \
                , A.At_least_one_Reportable_is_not_CC__c\
                , (SELECT Code__c, Name__c, Clarifier__c FROM RDC_Codes__r)\
                , (SELECT Code__c, Name__c, Clarifier__c, Anatomy__c, Severity__c FROM Patient_Codes__r)\
                , (SELECT CMPL123CME__Key_value__c FROM CMPL123CME__Questionnaires__r WHERE Reportable__c IN ('Yes'))\
                , (SELECT Id, Name FROM Letter_Requests__r WHERE Status__c NOT IN ('Cancelled'))\
        \
            FROM CMPL123CME__Complaint__c A\
            \
        WHERE Id IN (SELECT CMPL123CME__Complaint__c FROM CMPL123CME__Investigation__c WHERE CMPL123_WF_Status__c NOT IN ('Closed - Cancelled', 'Closed - Complete', 'Closed - Void'))\
                AND A.CMPL123CME__CMPL123_WF_Status__c IN ('Investigation in Progress')\
                AND A.CMPL123CME__Product__r.Product_Category_Formula__c NOT IN ('OCCL', 'SHDIST', 'SHTPM', 'SURG', 'TRCATH', 'VR')\
        "

    r = requests.get(sf.instance + sf.ext['query'] + '?q=' + SOQL.replace(' ', '+'), headers=sf.headers)
    df = pd.DataFrame(r.json()['records'])

    while 'nextRecordsUrl' in r.json().keys():
        r = requests.get(sf.instance + r.json()['nextRecordsUrl'], headers=sf.headers)
        df_temp = pd.DataFrame(r.json()['records'])
        df = pd.concat([df, df_temp])
    df.reset_index(drop=True, inplace=True)
        

    df['At_least_one_Reportable_is_not_CC__c'] = df['At_least_one_Reportable_is_not_CC__c'].map(lambda row: 'Yes' if row == 'Y' else 'No')
    df['Product Category'] = df['CMPL123CME__Product__r'].map(lambda row: row['Product_Category_Formula__c'])
    df['INV ID'] = df['CMPL123CME__Investigations__r'].map(lambda row: row['records'][0]['Id'])
    df['INV Name'] = df['CMPL123CME__Investigations__r'].map(lambda row: row['records'][0]['Name'])
    df['INV Owner'] = df['CMPL123CME__Investigations__r'].map(lambda row: row['records'][0]['OwnerId'])
    df['INV Owner'] = df['INV Owner'].map(lambda row: row if row != '00G1R000003TZSLUA4' else 'Investigation Queue')
    df['INV Status'] = df['CMPL123CME__Investigations__r'].map(lambda row: row['records'][0]['CMPL123_WF_Status__c'])
    df['RDC'] = df['RDC_Codes__r'].map(lambda row: combine_child_data(row, ['Code__c'], ', '))
    df['PC'] = df['Patient_Codes__r'].map(lambda row: combine_child_data(row, ['Code__c'], ', '))
    df['Max Severity'] = df['Patient_Codes__r'].map(lambda row: max([each['Severity__c'] if each['Severity__c'] != None else 0 for each in row['records']]))
    df['RDC Long'] = df['RDC_Codes__r'].map(lambda row: combine_child_data(row, ['Code__c', 'Name__c', 'Clarifier__c']))
    df['PC Long'] = df['Patient_Codes__r'].map(lambda row: combine_child_data(row, ['Code__c', 'Name__c', 'Clarifier__c', 'Anatomy__c']))
    df['Age of Complaint'] = df['Alert_Date__c'].map(lambda row: (datetime.now() - datetime.strptime(row, "%Y-%m-%d")).days)
    df['Age at EOM'] = df['Alert_Date__c'].map(lambda row: (date_EOM - datetime.strptime(row, "%Y-%m-%d")).days)
    df['Days in INV Queue'] = df['Investigation_Done_On__c'].map(lambda row: (datetime.now() - datetime.strptime(row[0:10], "%Y-%m-%d")).days)
    df['Reportable Tree'] = df['CMPL123CME__Questionnaires__r'].map(lambda row: combine_child_data(row, ['CMPL123CME__Key_value__c'], ', '))
    df['Reportable Tree'] = df['Reportable Tree'].str.replace('_Tree', '')
    df['LR ID'] = df['Letter_Requests__r'].map(lambda row: row['records'][0]['Id'] if row != None else '')
    df['LR Name'] = df['Letter_Requests__r'].map(lambda row: row['records'][0]['Name'] if row != None else '')
    df['Bucket'] = df['Product Category'].map(lambda row: assign_bucket(row))

    df['Priority Assignment'] = ''
    for i in range(len(df)):
        age_of_complaint = df.loc[i, 'Age of Complaint']
        days_in_inv_queue = df.loc[i, 'Days in INV Queue']
        age_EOM = df.loc[i, 'Age at EOM']
        df.loc[i, 'Priority Assignment'] = assign_priority(age_of_complaint, days_in_inv_queue, age_EOM)
        

    user_list = set(df['INV Owner'].to_list()) - {'Investigation Queue'}
    if len(user_list) > 0:
        user_list = "', '".join(user_list)
        SOQL = "SELECT Id, Name FROM User WHERE Id IN ('{}')".format(user_list)
        r = requests.get(sf.instance + sf.ext['query'] + '?q=' + SOQL.replace(' ', '+'), headers=sf.headers)
        df_user = pd.DataFrame(r.json()['records'])
        df_user = df_user.drop(columns='attributes')
        df_user = df_user.rename(columns={'Name': 'INV Owner Name', 'Id': 'INV Owner'})
        df = pd.merge(df, df_user, how='left', on='INV Owner')
        df['INV Owner Name'] = df['INV Owner Name'].map(lambda row: row if type(row) == str else 'Investigation Queue')
    else:
        df['INV Owner Name'] == 'Investigation Queue'

        
    df = df.drop(columns=['attributes', 
                        'CMPL123CME__Product__r', 
                        'CMPL123CME__Investigations__r', 
                        'RDC_Codes__r', 
                        'Patient_Codes__r', 
                        'Investigation_Done_On__c', 
                        'CMPL123CME__Questionnaires__r', 
                        'Letter_Requests__r'])

    columns_rename = {'Id': 'CN ID', 
                    'Name': 'CN Name',
                    'Product_Segment__c': 'Product Segment',
                    'Reportable_Country__c': 'Country', 
                    'Alert_Date__c': 'Alert Date',
                    'At_least_one_Reportable_is_not_CC__c': 'Reportable'}

    df = df.rename(columns=columns_rename)

    df = df[['Priority Assignment',
            'Bucket',
            'Product Category',
            'Age of Complaint',
            'Age at EOM',
            'Days in INV Queue',
            'Product Segment',
            'CN Name',
            'INV Name',
            'INV Owner Name',
            'LR Name',
            'Reportable',
            'Country',
            'Reportable Tree',
            'RDC',
            'PC',
            'Max Severity',
            'INV Owner',
            'INV Status',
            'RDC Long',
            'PC Long',
            'Alert Date',
            'CN ID',
            'INV ID',
            'LR ID',
            ]]


    df = df.sort_values(by=['Priority Assignment', 'Age of Complaint', 'Days in INV Queue'], ascending=[True, False, False])

    if option == 'queue':
        df = df[df['INV Owner Name'] == 'Investigation Queue']
    elif option == 'assigned':
        df = df[df['INV Owner Name'] != 'Investigation Queue']

    df.reset_index(drop=True, inplace=True)

    return df