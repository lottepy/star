
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
from io import StringIO
import pandas as pd

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
# jobId = "VjF8fDQ4NzU5MDk3MA"
# requestUrl = "https://hosted.datascopeapi.reuters.com/RestApi/v1/Extractions/ExtractedFiles" + "('" + jobId + "')" + "/$value"
# # requestUrl="https://hosted.datascopeapi.reuters.com/RestApi/v1/Extractions/Schedules(0x06a4c7c660e09591)/LastExtraction"
# # https://hosted.datascopeapi.reuters.com/RestApi/v1/Extractions/ScheduleGetByName(ScheduleName='example-eod')
#
# requestHeaders={
#     "Prefer":"respond-async",
#     "Content-Type":"application/json",
#     "Authorization": "token " + token,
#     "Accept-Encoding":"gzip",
#     "Accept-Charset":"UTF-8",
# }
#
# requestBody={
#     # "ScheduleName":'example-eod'
# }
#
# r2 = requests.post(requestUrl, json=requestBody,headers=requestHeaders)
#
# #Display the HTTP status of the response
# #Initial response status (after approximately 30 seconds wait) is usually 202
# status_code = r2.status_code
# print ("HTTP status of the response: " + str(status_code))


# In[8]:

#Step 3: if required, poll the status of the request using the received location URL.
#Once the request has completed, retrieve the jobId and extraction notes.

#If status is 202, display the location url we received, and will use to poll the status of the extraction request:
# if status_code == 202 :
#     requestUrl = r2.headers["location"]
#     print ('Extraction is not complete, we shall poll the location URL:')
#     print (str(requestUrl))
#
#     requestHeaders={
#         "Prefer":"respond-async",
#         "Content-Type":"application/json",
#         "Authorization":"token " + token
#     }
#
# #As long as the status of the request is 202, the extraction is not finished;
# #we must wait, and poll the status until it is no longer 202:
# while (status_code == 202):
#     print ('As we received a 202, we wait 30 seconds, then poll again (until we receive a 200)')
#     time.sleep(5)
#     r3 = requests.get(requestUrl,headers=requestHeaders)
#     status_code = r3.status_code
#     print ('HTTP status of the response: ' + str(status_code))

#When the status of the request is 200 the extraction is complete;
#we retrieve and display the jobId and the extraction notes (it is recommended to analyse their content)):

# if status_code == 200 :
#     r2Json = json.loads(r2.text.encode('ascii', 'ignore'))
#     jobId = r2Json["JobId"]
#     print ('\njobId: ' + jobId + '\n')
#     notes = r2Json["Notes"]
#     print ('Extraction notes:\n' + notes[0])
#
# #If instead of a status 200 we receive a different status, there was an error:
# if status_code != 200 :
#     print ('An error occured. Try to run this cell again. If it fails, re-run the previous cell.\n')


# In[9]:

#Step 4: get the extraction results, using the received jobId.
#Decompress the data and display it on screen.
#Skip this step if you asked for a large data set, and go directly to step 5 !

#We also save the data to disk; but note that if you use AWS it will be saved as a GZIP,
#otherwise it will be saved as a CSV !
#This discrepancy occurs because we allow automatic decompression to happen when retrieving
#from TRTH, so we end up saving the decompressed contents.

#IMPORTANT NOTE:
#The code in this step is only for demo, to display some data on screen.
#Avoid using this code in production, it will fail for large data sets !
#See step 5 for production code.
jobId = "VjF8fDQ4NzU5MDk3MA"
requestUrl = "https://hosted.datascopeapi.reuters.com/RestApi/v1/Extractions/ExtractedFiles" + "('" + jobId + "')" + "/$value"
# Extractions/ExtractedFiles({id})/$value
#AWS requires an additional header: X-Direct-Download
if useAws:
    requestHeaders={
        "Prefer":"respond-async",
        "Content-Type":"text/plain",
        "Accept-Encoding":"gzip",
        "X-Direct-Download":"true",
        "Authorization": "token " + token
    }
else:
    requestHeaders={
        "Prefer":"respond-async",
        # "Content-Type":"text/plain",
        "Content-Type":"application/json",
        "Accept-Encoding":"UTF-8",
        "Authorization": "token " + token
    }

r4 = requests.get(requestUrl,headers=requestHeaders)
data = pd.read_csv(StringIO(r4.text))




