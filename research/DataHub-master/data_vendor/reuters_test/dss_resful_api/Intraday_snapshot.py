# Set these parameters before running the code:
# https://hosted.datascopeapi.reuters.com/RestApi.Help/Home/RestApiProgrammingSdk
import pandas as pd
from io import StringIO
import gzip
import urllib3
import time
import shutil
import json
import requests
myUsername = "9022268"
myPassword = "nP8F&nnau.U4"
# myUsername = "9021920"
# myPassword = "V4e9DC](UaJs"
urllib3.disable_warnings()


# Step 1: token request
# requestUrl = "https://192.165.219.152/RestApi/v1/Authentication/RequestToken"
requestUrl = "https://hosted.datascopeapi.reuters.com/RestApi/v1/Authentication/RequestToken"

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


# Step 2: send an on demand extraction request using the received token

# requestUrl = 'https://192.165.219.152/RestApi/v1/Extractions/ExtractWithNotes'
requestUrl = "https://hosted.datascopeapi.reuters.com/RestApi/v1/Extractions/ExtractWithNotes"

requestHeaders = {
    "Prefer": "respond-async",
    "Content-Type": "application/json",
    "Authorization": "token " + token
}

requestBody =  {
  "ExtractionRequest": {
    "@odata.type": "#ThomsonReuters.Dss.Api.Extractions.ExtractionRequests.IntradayPricingExtractionRequest",
    "ContentFieldNames": [
      # "RIC",
      "Ask Price",
      "Asset Type",
      "Bid Price",
      "Currency Code",
      "Exchange Code",
      "High Price",
      "Instrument ID",
      "Instrument ID Type",
      "Low Price",
      "Open Price",
      # "Previous Close Date",
      # "Previous Close Price",
      "Security Description",
      "Settlement Price",
      "Trade Date",
      # "User Defined Identifier",
      "Volume",
      "Last Volume",
    "Latest Trade Time Milliseconds",
    "Last Trade Price Timestamp",
    "Last Price",
    "Best Ask Price",
    "Best Bid Price",
    "Last Update Time"
    ],
    "IdentifierList": {
      "@odata.type": "#ThomsonReuters.Dss.Api.Extractions.ExtractionRequests.InstrumentIdentifierList",
      "InstrumentIdentifiers": [
      	# { "Identifier": "438516AC0", "IdentifierType": "Cusip" },
      	# { "Identifier": "AGG", "IdentifierType": "Ric" },
        # {"Identifier": "JPST.L", "IdentifierType": "Ric"},
        # {"Identifier": "IBM.N", "IdentifierType": "Ric"},
        #   {"Identifier": "000001.SZ", "IdentifierType": "Ric"}
        #  {"Identifier": "0700.HK", "IdentifierType": "Ric"},
        #  {"Identifier": "HMHM9", "IdentifierType": "Ric"}
        #  {"Identifier": "VXK9", "IdentifierType": "Ric"},
         {"Identifier": "EUR=", "IdentifierType": "Ric"},
         {"Identifier": "JPY=", "IdentifierType": "Ric"}
      ]
    },
    "Condition": {
        "ScalableCurrency": False,
        "OnlyNonEmbargoedData": True  # 此处，证明我们没有购买
        # 需要等待 15 minutes required by [ HS1 (HKEX-HONG KONG SE LEVEL 1)
    }
  }
}

r2 = requests.post(requestUrl, json=requestBody, headers=requestHeaders, verify=False)

# Display the HTTP status of the response
# Initial response status (after approximately 30 seconds wait) is usually 202
status_code = r2.status_code
print("HTTP status of the response: " + str(status_code))


# In[8]:

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
    print("r2Json['Notes']: {}".format(r2Json['Notes']))

# If instead of a status 200 we receive a different status, there was an error:
if status_code != 200:
    print('An error occured. Try to run this cell again. If it fails, re-run the previous cell.\n')
