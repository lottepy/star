
# coding: utf-8

# In[2]:

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
myUsername = "9022268"
myPassword = "nP8F&nnau.U4"
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

requestUrl='https://hosted.datascopeapi.reuters.com/RestApi/v1/Search/InstrumentSearch'

requestHeaders={
    "Prefer":"respond-async",
    "Content-Type":"application/json",
    "Authorization": "token " + token
}

requestBody={
  "SearchRequest": {
    "InstrumentTypeGroups": [
      # "CollatetizedMortgageObligations",
      # "Commodities",
      "Equities",
      # "FuturesAndOptions",
      # "GovCorp",
      # "MortgageBackedSecurities",
      "Money",
      # "Municipals",
      # "Funds"
    ],
    # "IdentifierType": "Ric",
    "IdentifierType": "Ticker",
    "Identifier": "ACWI,JPST,MINT",
    "PreferredIdentifierType": "Ric"
  }
}

r2 = requests.post(requestUrl, json=requestBody,headers=requestHeaders)

#Display the HTTP status of the response
#Initial response status (after approximately 30 seconds wait) is usually 202
status_code = r2.status_code
print ("HTTP status of the response: " + str(status_code))

r2Json = json.loads(r2.text.encode('ascii', 'ignore'))
result = r2Json.get('value')
print(result)


