#!/usr/bin/env python
# coding: utf-8

# In[8]:


from get_price_history import get_pricehis
from get_corporate_action import get_corpo


# In[9]:


fields=[
      "File Code",
      "RIC",
      "Trade Date",
      "Last Trade Price",
      "Universal Close Price",
      "Alternate Close Price",
      "High Price",
      "Low Price",
      "Open Price",
      "Volume Weighted Average Price",
      "Turnover",
      "Volume",
      "Accumulated Volume Unscaled",
      "Bid Price",
      "Asset Type"]
tickers=["438516AC0","ALVG.DE", "RTR.L", "IBM.N"]
ti_type=["Cusip","Ric","Ric","Ric"]
start="2006-05-24T00:00:00.000Z"
end="2006-05-31T00:00:00.000Z"

a=get_pricehis(tickers,ti_type,fields,start,end)
a


# In[10]:


fields1=[
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

    tickers1=["438516AC0","ALVG.DE", "RTR.L", "IBM.N"]
ti_type1=["Cusip","Ric","Ric","Ric"]
get_corpo(tickers1,ti_type1,fields1)


# In[ ]:





# In[ ]:





# In[ ]:




