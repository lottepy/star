
# coding: utf-8

# In[2]:

# ====================================================================================
# On Demand request demo code, using the following steps:
# - Authentication token request
# - On Demand extraction request
# - Extraction status polling request
#   Extraction notes retrieval
# - Data retrieval and save to disk (the data file is gzipped)
#   Includes AWS download capability
# ====================================================================================

# Set these parameters before running the code:
import pandas as pd
from io import StringIO
import gzip
import urllib3
import time
import shutil
import json
import requests
filePath = "/Users/dong/Downloads/"  # Location to save downloaded files
fileNameRoot = "Python_Test"  # Root of the name for the downloaded files
myUsername = "9022268"
myPassword = "nP8F&nnau.U4"
useAws = False
# Set the last parameter above to:
# False to download from TRTH servers
# True to download from Amazon Web Services cloud (recommended, it is faster)

# Imports:

urllib3.disable_warnings()

# ====================================================================================
# Step 1: token request

requestUrl = "https://192.165.219.152/RestApi/v1/Authentication/RequestToken"

requestHeaders = {
    "Prefer": "respond-async",
    "Content-Type": "application/json"
    }

requestBody = {
    "Credentials": {
        "Username": myUsername,
        "Password": myPassword,
        }
}

r1 = requests.post(requestUrl, json=requestBody, headers=requestHeaders, verify=False)

if r1.status_code == 200:
    jsonResponse = json.loads(r1.text.encode('ascii', 'ignore'))
    token = jsonResponse["value"]
    print('Authentication token (valid 24 hours):')
    print(token)
else:
    print('Replace myUserName and myPassword with valid credentials, then repeat the request')


# In[7]:

# Step 2: send an on demand extraction request using the received token

requestUrl = 'https://192.165.219.152/RestApi/v1/Extractions/ExtractWithNotes'

requestHeaders = {
    "Prefer": "respond-async",
    "Content-Type": "application/json",
    "Authorization": "token " + token
}

requestBody = {
    "ExtractionRequest": {
        # "@odata.type": "#ThomsonReuters.Dss.Api.Extractions.ExtractionRequests.TickHistoryIntradaySummariesExtractionRequest",
        "@odata.type": "#ThomsonReuters.Dss.Api.Extractions.ExtractionRequests.TimeSeriesExtractionRequest",
        "ContentFieldNames": [
            # "File Code",
            # "RIC",
            "Trade Date",
            "Close Price",
            # "Alternate Close Price",
            # "Split Factor",
            # "High Price",
            # "Low Price",
            # "Open Price",
            # "VWAP Price",
            # "Turnover",
            "Volume",
            # "Bid Price",
            # "Asset Type",
            # "Quote ID",
            # "Bid Yield",
            "Universal Close Price",
            # "Exchange Code",
            # "Currency Code",
            ],
        "IdentifierList": {
            "@odata.type": "#ThomsonReuters.Dss.Api.Extractions.ExtractionRequests.InstrumentIdentifierList",
            "InstrumentIdentifiers": [
                {"Identifier": "JPST.L", "IdentifierType": "Ric"},
                # {"Identifier": "IBM.N", "IdentifierType": "Ric"}
                ]
            },
        "Condition": {
            "LastPriceOnly": False,
            "StartDate": "2019-05-01T00:00:00.000Z",
            "EndDate": "2019-05-15T23:59:59.999Z"
            }
        }
}

r2 = requests.post(requestUrl, json=requestBody, headers=requestHeaders, verify=False)

# Display the HTTP status of the response
# Initial response status (after approximately 30 seconds wait) is usually 202
status_code = r2.status_code
print("HTTP status of the response: " + str(status_code))


# In[8]:.

# Step 3: if required, poll the status of the request using the received location URL.
# Once the request has completed, retrieve the jobId and extraction notes.

# If status is 202, display the location url we received, and will use to poll the status of the extraction request:
# if status_code == 202:
#     requestUrl = r2.headers["location"]
#     print('Extraction is not complete, we shall poll the location URL:')
#     print(str(requestUrl))
#
#     requestHeaders = {
#         "Prefer": "respond-async",
#         "Content-Type": "application/json",
#         "Authorization": "token " + token
#     }
#
# # #As long as the status of the request is 202, the extraction is not finished;
# # #we must wait, and poll the status until it is no longer 202:
# while (status_code == 202):
#     print('As we received a 202, we wait 30 seconds, then poll again (until we receive a 200)')
#     time.sleep(5)
#     r3 = requests.get(requestUrl, headers=requestHeaders)
#     status_code = r3.status_code
#     print('HTTP status of the response: ' + str(status_code))

# When the status of the request is 200 the extraction is complete;
# we retrieve and display the jobId and the extraction notes (it is recommended to analyse their content)):

if status_code == 200:
    r2Json = json.loads(r2.text.encode('ascii', 'ignore'))
    print("r2Json.keys(): {}".format(r2Json.keys()))
    print("r2Json['Contents']: {}".format(r2Json['Contents']))

# If instead of a status 200 we receive a different status, there was an error:
if status_code != 200:
    print('An error occured. Try to run this cell again. If it fails, re-run the previous cell.\n')

# HTTP status of the response: 200
# r2Json.keys(): dict_keys(['@odata.context', 'Contents', 'Notes'])
# r2Json['Contents']: [{'IdentifierType': 'Ric', 'Identifier': 'ALVG.DE', 'Volume': 1222145}]


# HTTP status of the response: 200
# r2Json.keys(): dict_keys(['@odata.context', 'Contents', 'Notes'])
# r2Json['Contents']: [{'IdentifierType': 'Ric', 'Identifier': 'ALVG.DE', 'Trade Date': '2019-05-13', 'Volume': 1222145}]