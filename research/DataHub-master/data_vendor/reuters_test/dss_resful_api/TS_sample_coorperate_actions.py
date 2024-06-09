
# coding: utf-8



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
        "@odata.type": "#ThomsonReuters.Dss.Api.Extractions.ExtractionRequests.CorporateActionsStandardExtractionRequest",
        "ContentFieldNames": [
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
            ],
        "IdentifierList": {
            "@odata.type": "#ThomsonReuters.Dss.Api.Extractions.ExtractionRequests.InstrumentIdentifierList",
            "InstrumentIdentifiers": [
                # {"Identifier": "438516AC0", "IdentifierType": "Cusip"},
                # {"Identifier": "ALVG.DE", "IdentifierType": "Ric"},
                {"Identifier": "IBM.N", "IdentifierType": "Ric"}
            ]
            },
        "Condition": {
            # "ReportDateRangeType": "Last",
            # "PreviousDays": 5,
            "ReportDateRangeType": "Range",
            "QueryStartDate": "2017-09-11T00:00:00.000Z",
            "QueryEndDate": "2017-09-13T23:59:59.000Z",
            "ExcludeDeletedEvents": True,
            "IncludeCapitalChangeEvents": True,
            "IncludeDividendEvents": True,
            "IncludeEarningsEvents": False,
            "IncludeMergersAndAcquisitionsEvents": False,
            "IncludeNominalValueEvents": False,
            "IncludePublicEquityOfferingsEvents": False,
            "IncludeSharesOutstandingEvents": False,  # 流通股
            "IncludeVotingRightsEvents": False,
            "CorporateActionsCapitalChangeType": "CapitalChangeExDate",
            "CorporateActionsDividendsType": "DividendPayDate",
            "CorporateActionsEarningsType": "PeriodEndDate",
            "ShareAmountTypes": [
                ]
            }
        }
}

r2 = requests.post(requestUrl, json=requestBody, headers=requestHeaders, verify=False)

# Display the HTTP status of the response
# Initial response status (after approximately 30 seconds wait) is usually 202
status_code = r2.status_code
print("HTTP status of the response: " + str(status_code))


if status_code == 200:
    r2Json = json.loads(r2.text.encode('ascii', 'ignore'))
    print("r2Json.keys(): {}".format(r2Json.keys()))
    print("r2Json['Contents']: {}".format(r2Json['Contents']))

# If instead of a status 200 we receive a different status, there was an error:
if status_code != 200:
    print('An error occured. Try to run this cell again. If it fails, re-run the previous cell.\n')

"""
参考资料：
1. 官方： https://developers.refinitiv.com/datascope-select-dss/datascope-select-rest-api/learning?content=5986&type=learning_material_item
2. https://community.developers.refinitiv.com/questions/18507/corporate-action-data-retrieval-stored-sheduled-ap.html
3. https://community.developers.refinitiv.com/questions/18656/corporate-actions-how-to-get-the-closest-shares-am.html
"""
