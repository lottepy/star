
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
filePath = "/Users/u8007966/Downloads/"  #Location to save downloaded files
fileNameRoot = "Python_Test"     #Root of the name for the downloaded files
myUsername = "myUsername"
myPassword = "myPassword"
useAws = True
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
    # "Username": myUsername,
    # "Password": myPassword,
    "Username" : "9022268",
    "Password" : "nP8F&nnau.U4",
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

requestUrl='https://hosted.datascopeapi.reuters.com/RestApi/v1/Extractions/ExtractRaw'

requestHeaders={
    "Prefer":"respond-async",
    "Content-Type":"application/json",
    "Authorization": "token " + token
}

requestBody={
  "ExtractionRequest": {
    # "@odata.type": "#ThomsonReuters.Dss.Api.Extractions.ExtractionRequests.TickHistoryIntradaySummariesExtractionRequest",
    "@odata.type": "#ThomsonReuters.Dss.Api.Extractions.ExtractionRequests.Intraday Pricing ReportExtractionRequest",
    "ContentFieldNames": [
      "Close Ask",
      "Close Bid",
      "High",
      "High Ask",
      "High Bid",
      "Last",
      "Low",
      "Low Ask",
      "Low Bid",
      "No. Asks",
      "No. Bids",
      "No. Trades",
      "Open",
      "Open Ask",
      "Open Bid",
      "Volume"
    ],
    "IdentifierList": {
      "@odata.type": "#ThomsonReuters.Dss.Api.Extractions.ExtractionRequests.InstrumentIdentifierList",  
      "InstrumentIdentifiers": [{
        "Identifier": "CARR.PA",
        "IdentifierType": "Ric"
      },{
        "Identifier": "IBM.N",
        "IdentifierType": "Ric"
      }],
      "UseUserPreferencesForValidationOptions":"false"
    },    
    "Condition": {
      "MessageTimeStampIn": "GmtUtc",
      "ReportDateRangeType": "Range",
      "QueryStartDate": "2016-09-29T00:00:00.000Z",
      "QueryEndDate": "2016-09-30T00:00:00.000Z",
      "SummaryInterval": "OneHour",
      "TimebarPersistence":"true",
      "DisplaySourceRIC":"true"
    }
  }
}

r2 = requests.post(requestUrl, json=requestBody,headers=requestHeaders)

#Display the HTTP status of the response
#Initial response status (after approximately 30 seconds wait) is usually 202
status_code = r2.status_code
print ("HTTP status of the response: " + str(status_code))


# In[8]:

#Step 3: if required, poll the status of the request using the received location URL.
#Once the request has completed, retrieve the jobId and extraction notes.

#If status is 202, display the location url we received, and will use to poll the status of the extraction request:
if status_code == 202 :
    requestUrl = r2.headers["location"]
    print ('Extraction is not complete, we shall poll the location URL:')
    print (str(requestUrl))
    
    requestHeaders={
        "Prefer":"respond-async",
        "Content-Type":"application/json",
        "Authorization":"token " + token
    }

#As long as the status of the request is 202, the extraction is not finished;
#we must wait, and poll the status until it is no longer 202:
while (status_code == 202):
    print ('As we received a 202, we wait 30 seconds, then poll again (until we receive a 200)')
    time.sleep(30)
    r3 = requests.get(requestUrl,headers=requestHeaders)
    status_code = r3.status_code
    print ('HTTP status of the response: ' + str(status_code))

#When the status of the request is 200 the extraction is complete;
#we retrieve and display the jobId and the extraction notes (it is recommended to analyse their content)):
if status_code == 200 :
    r3Json = json.loads(r3.text.encode('ascii', 'ignore'))
    jobId = r3Json["JobId"]
    print ('\njobId: ' + jobId + '\n')
    notes = r3Json["Notes"]
    print ('Extraction notes:\n' + notes[0])

#If instead of a status 200 we receive a different status, there was an error:
if status_code != 200 :
    print ('An error occured. Try to run this cell again. If it fails, re-run the previous cell.\n')


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

requestUrl = "https://hosted.datascopeapi.reuters.com/RestApi/v1/Extractions/RawExtractionResults" + "('" + jobId + "')" + "/$value"

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
        "Content-Type":"text/plain",
        "Accept-Encoding":"gzip",
        "Authorization": "token " + token
    }

r4 = requests.get(requestUrl,headers=requestHeaders)
if useAws:
    print ('Content response headers (AWS server): type: ' + r4.headers["Content-Type"] + '\n')
    #AWS does not set header Content-Encoding="gzip", so the requests call does not decompress.
    #We therefore decompress using a separate call (to the gzip library).
    uncompressedData = gzip.decompress(r4.content).decode("utf-8") 
    #We save the original compressed data (to save space):
    fileName = filePath + fileNameRoot + ".step4.csv.gz"
    print ('Saving compressed data to file: ' + fileName +  ' ... please be patient')
else:
    print ('Content response headers (TRTH server): type: ' + r4.headers["Content-Type"] + ' - encoding: ' + r4.headers["Content-Encoding"] + '\n')
    #The requests call automatically decompresses the data, if header Content-Encoding="gzip".
    uncompressedData = r4.text
    #We save the uncompressed data (because it was automatically decompressed):
    fileName = filePath + fileNameRoot + ".step4.csv"
    print ('Saving uncompressed data to file: ' + fileName +  ' ... please be patient')

#Save to file:
with open(fileName, 'wb') as fd:
    for chunk in r4.iter_content(chunk_size=1024):
        fd.write(chunk)
fd.close
print ('Finished saving data to file:' + fileName + '\n')

#Display data:
print ('Decompressed data:\n' + uncompressedData)

#Note: variable uncompressedData stores all the data.
#This is not a good practice, that can lead to issues with large data sets.
#We only use it here as a convenience for the demo, to keep the code very simple.


# In[10]:

#Step 5: get the extraction results, using the received jobId.
#We also save the compressed data to disk, as a GZIP.
#We only display a few lines of the data.

#IMPORTANT NOTE:
#This code is much better than that of step 4; it should not fail even with large data sets.
#If you need to manipulate the data, read and decompress the file, instead of decompressing
#data from the server on the fly.
#This is the recommended way to proceed, to avoid data loss issues.
#For more information, see the related document:
#   Advisory: avoid incomplete output - decompress then download

requestUrl = "https://hosted.datascopeapi.reuters.com/RestApi/v1/Extractions/RawExtractionResults" + "('" + jobId + "')" + "/$value"

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
        "Content-Type":"text/plain",
        "Accept-Encoding":"gzip",
        "Authorization": "token " + token
    }

r5 = requests.get(requestUrl,headers=requestHeaders,stream=True)
#Ensure we do not automatically decompress the data on the fly:
r5.raw.decode_content = False
if useAws:
    print ('Content response headers (AWS server): type: ' + r5.headers["Content-Type"] + '\n')
    #AWS does not set header Content-Encoding="gzip".
else:
    print ('Content response headers (TRTH server): type: ' + r5.headers["Content-Type"] + ' - encoding: ' + r5.headers["Content-Encoding"] + '\n')

#Next 2 lines display some of the compressed data, but if you uncomment them save to file fails
#print ('20 bytes of compressed data:')
#print (r5.raw.read(20))

print ('Saving compressed data to file:' + fileName + ' ... please be patient')
fileName = filePath + fileNameRoot + ".step5.csv.gz"
chunk_size = 1024
rr = r5.raw
with open(fileName, 'wb') as fd:
    shutil.copyfileobj(rr, fd, chunk_size)
fd.close

print ('Finished saving compressed data to file:' + fileName + '\n')

#Now let us read and decompress the file we just created.
#For the demo we limit the treatment to a few lines:
maxLines = 10
print ('Read data from file, and decompress at most ' + str(maxLines) + ' lines of it:')

uncompressedData = ""
count = 0
with gzip.open(fileName, 'rb') as fd:
    for line in fd:
        dataLine = line.decode("utf-8")
        #Do something with the data:
        print (dataLine)
        uncompressedData = uncompressedData + dataLine
        count += 1
        if count >= maxLines:
            break
fd.close()

#Note: variable uncompressedData stores all the data.
#This is not a good practice, that can lead to issues with large data sets.
#We only use it here as a convenience for the next step of the demo, to keep the code very simple.
#In production one would handle the data line by line (as we do with the screen display)


# In[11]:

#Step 6 (cosmetic): formating the response received in step 4 or 5 using a panda dataframe

from io import StringIO
import pandas as pd

timeSeries = pd.read_csv(StringIO(uncompressedData))
timeSeries


# In[ ]:



