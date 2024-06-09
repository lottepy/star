#!/usr/bin/env python
# coding: utf-8

# In[28]:



def get_corpo(tickers,ti_type,fields):
    filePath = "/Users/zhangjunting/"  #Location to save downloaded files
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
    import pandas as pd
    
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
    print (r1.status_code)
    if r1.status_code == 200 :
        jsonResponse = json.loads(r1.text.encode('ascii', 'ignore'))
        token = jsonResponse["value"]
        #print ('Authentication token (valid 24 hours):')
        #print (token)
    else:
        #print ('Replace myUserName and myPassword with valid credentials, then repeat the request')

    iden_list=[]
    for i in range(0,len(tickers)):
        j={"Identifier": tickers[i],"IdentifierType": ti_type[i]}
        iden_list.append(j)
    requestUrl="https://hosted.datascopeapi.reuters.com/RestApi/v1/Extractions/ExtractWithNotes"
    requestHeaders={
        "Prefer":"respond-async",
        "Content-Type":"application/json",
        "Authorization": "Token " + token
    }
    requestBody={
      "ExtractionRequest": {
        "@odata.type": "#ThomsonReuters.Dss.Api.Extractions.ExtractionRequests.CorporateActionsStandardExtractionRequest",
        "ContentFieldNames":fields,
        "IdentifierList": {
          "@odata.type": "#ThomsonReuters.Dss.Api.Extractions.ExtractionRequests.InstrumentIdentifierList",
          "InstrumentIdentifiers": iden_list
        },
        "Condition": {
          "ReportDateRangeType": "Last",
          "PreviousDays": 30,
          "ExcludeDeletedEvents": True,
          "IncludeCapitalChangeEvents": True,
          "IncludeDividendEvents": True,
          "IncludeEarningsEvents": True,
          "IncludeMergersAndAcquisitionsEvents": True,
          "IncludeNominalValueEvents": True,
          "IncludePublicEquityOfferingsEvents": True,
          "IncludeSharesOutstandingEvents": True,
          "IncludeVotingRightsEvents": True,
          "CorporateActionsCapitalChangeType": "CapitalChangeExDate",
          "CorporateActionsDividendsType": "DividendPayDate",
          "CorporateActionsEarningsType": "PeriodEndDate",
          "ShareAmountTypes": [
          ]
        }
      }
    }
    r2 = requests.post(requestUrl, json=requestBody,headers=requestHeaders)
    status_code = r2.status_code
    #print(r2.content)
    r2Json = json.loads(r2.text.encode('ascii', 'ignore'))
    #print(r2Json)
    content= r2Json['Contents']
   # print ("HTTP status of the response: " + str(status_code))
    df = pd.DataFrame(content)
    return df



# In[29]:


fields=[
      "Corporate Actions Type",
      "Capital Change Event Type",
      "Capital Change Event Type Description",
      "Actual Adjustment Type",
      "Actual Adjustment Type Description",
      "Adjustment Factor",
      "Currency Code",
      "Exchange Code",
      "Effective Date",
      "Dividend Pay Date",
      "Dividend Rate",
      "Nominal Value",
      "Nominal Value Currency",
      "Nominal Value Date"
    ]

tickers=["438516AC0","ALVG.DE", "RTR.L", "IBM.N"]
ti_type=["Cusip","Ric","Ric","Ric"]
get_corpo(tickers,ti_type,fields)


# In[ ]:





# In[ ]:




