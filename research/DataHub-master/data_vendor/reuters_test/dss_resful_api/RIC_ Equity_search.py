# coding: utf-8
# https://developers.refinitiv.com/datascope-select-dss/datascope-select-rest-api/learning?content=10902&type=learning_material_item
import pandas as pd
# In[2]:

# ticker_list = [x.replace("GB_10_","") for x in pd.read_csv('GB_10_RIC.csv')['IUID'].tolist()]

#====================================================================================
#On Demand request demo code, using the following steps:
# - Authentication token request
# - On Demand extraction request
# - Extraction status polling request
#   Extraction notes retrieval
# - Data retrieval and save to disk (the data file is gzipped)
#   Includes AWS download capability
#====================================================================================

#Set these parameters before running the code:
filePath = "/Users/Ryan/Downloads/"  #Location to save downloaded files
fileNameRoot = "Python_Test"     #Root of the name for the downloaded files
# myUsername = "9022268"
# myPassword = "nP8F&nnau.U4"

myUsername = "9021920"
myPassword = "V4e9DC](UaJs"
useAws = False
#Set the last parameter above to:
# False to download from TRTH servers
# True to download from Amazon Web Services cloud (recommended, it is faster)

#Imports:
import requests
import json
import shutil
import time
import urllib3
import gzip

#====================================================================================
#Step 1: token request

requestUrl = "https://hosted.datascopeapi.reuters.com/RestApi/v1/Authentication/RequestToken"

requestHeaders={
    "Prefer":"respond-async",
    "Content-Type":"application/json"
    }

requestBody={
    "Credentials": {
    "Username": myUsername,
    "Password": myPassword,
}
}

r1 = requests.post(requestUrl, json=requestBody,headers=requestHeaders)

if r1.status_code == 200 :
    jsonResponse = json.loads(r1.text.encode('ascii', 'ignore'))
    token = jsonResponse["value"]
    print ('Authentication token (valid 24 hours):')
    print (token)
else:
    print ('Replace myUserName and myPassword with valid credentials, then repeat the request')


# In[7]:

#Step 2: send an on demand extraction request using the received token 
def ric_equity_search(ticker = "MINT", Currency = "USD"):
    requestUrl='https://hosted.datascopeapi.reuters.com/RestApi/v1/Search/EquitySearch'

    requestHeaders={
        "Prefer":"respond-async",
        "Content-Type":"application/json",
        "Authorization": "token " + token
    }
    null = None
    requestBody={
        "SearchRequest": {
            "AssetStatus": "Active",
            "AssetCategoryCodes": ["ORD"],
            "SubTypeCodes": null,
            # "CurrencyCodes": ["USD"],
            "CompanyName": null,
            "Description": null,
            # "DomicileCodes": ["IE"],
            "ExchangeCodes": [ "HKG"],
            "FairValueIndicator": null,
            "FileCodes": null,
            "GicsCodes": null,
            "OrgId": null,
            "Ticker": ticker,
            "Identifier": null,
            "IdentifierType": null,
            "PreferredIdentifierType": null
        }
    }

    r2 = requests.post(requestUrl, json=requestBody,headers=requestHeaders)

    #Display the HTTP status of the response
    #Initial response status (after approximately 30 seconds wait) is usually 202
    status_code = r2.status_code
    print ("HTTP status of the response: " + str(status_code))

    r2Json = json.loads(r2.text.encode('ascii', 'ignore'))
    result = r2Json.get('value')
    return pd.DataFrame(result)
result = ric_equity_search('*')

RIC_dict = {}
RIC_df = pd.DataFrame()
for ticker in ticker_list:
    RIC_data_df = ric_equity_search(ticker)
    RIC_data_df['Ticker'] = ticker
    RIC_df = RIC_df.append(RIC_data_df)
    RIC_dict[ticker] = RIC_data_df['Identifier']

print(RIC_df.head())





