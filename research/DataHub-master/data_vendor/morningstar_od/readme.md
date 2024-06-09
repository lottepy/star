
# Morningstar API python methods

2018-10-20


```python
from fundamental_api import FundamentalAPI
from base_api import MasterID
```

    /Users/joseph/miniconda2/envs/morningstarapi/lib/python3.6/site-packages/ipykernel_launcher.py:1: DtypeWarning: Columns (14,36,37,38,47,48,52,54,57,58,59,67,71,75,76,82,97) have mixed types. Specify dtype option on import or set low_memory=False.
      """Entry point for launching an IPython kernel.



```python
isin = 'IE00B11XYX59'
```


```python
mid = MasterID()
secid = mid.from_isin_to_secid(isin = isin)
api = FundamentalAPI(secid = secid)
```


```python
api.get_inception_date()
```




    '2006-03-31'




```python
api.get_manager_list()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>StartDate</th>
      <th>GivenName</th>
      <th>MiddleName</th>
      <th>FamilyName</th>
      <th>CareerStartYear</th>
      <th>College</th>
      <th>Biograhy</th>
      <th>EndDate</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>2</th>
      <td>2006-10-29</td>
      <td>Curtis</td>
      <td>A.</td>
      <td>Mewbourne</td>
      <td>1991</td>
      <td>University of Pennsylvania</td>
      <td>Mr. Mewbourne is a managing director and head ...</td>
      <td>2008-10-31</td>
    </tr>
    <tr>
      <th>3</th>
      <td>2008-11-03</td>
      <td>Andrew</td>
      <td>None</td>
      <td>Bosomworth</td>
      <td>None</td>
      <td>None</td>
      <td>Mr. Bosomworth is a managing director in the M...</td>
      <td>2009-03-31</td>
    </tr>
    <tr>
      <th>0</th>
      <td>2009-04-01</td>
      <td>Francesc</td>
      <td>None</td>
      <td>Balcells</td>
      <td>1996</td>
      <td>Johns Hopkins University (Paul H. Nitze)</td>
      <td>Mr. Balcells is an executive vice president an...</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>4</th>
      <td>2009-04-01</td>
      <td>Michael</td>
      <td>A.</td>
      <td>Gomez</td>
      <td>1995</td>
      <td>University of Pennsylvania (Wharton)</td>
      <td>Mr. Gomez is a managing director in the Newpor...</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2017-05-01</td>
      <td>Yacov</td>
      <td>None</td>
      <td>Arnopolin</td>
      <td>2000</td>
      <td>Carnegie Mellon University</td>
      <td>Mr. Arnopolin is an executive vice president a...</td>
      <td>NaN</td>
    </tr>
  </tbody>
</table>
</div>




```python
api.get_investment_obj()
```




    'Objective The fund aims to maximise the total return on your investment through primarily investing in a range of fixed income securities and instruments issued by companies or governments from emerging markets around the world, using prudent investment management principles. Investment Policy The fund aims to achieve its objective by investing primarily in a range of fixed income securities and instruments (which are loans that pay a fixed or variable rate of interest). The fund will mainly invest in emerging markets, which in investment terms are those economies that are not as developed. The fund is likely to concentrate its investments in Asia, Africa, the Middle East, Latin America and the developing countries of Europe. The average portfolio duration of this fund will normally be within two years (plus or minus) of the JP Morgan Emerging Markets Bond Index (EMBI) Global. Duration measures the sensitivity of the assets to interest rate risk. The longer the duration the higher the sensitivity to changes in interest rates. The fund may invest in “investment grade” and “non-investment grade securities”. Non-investment grade securities are considered to be more risky, but typically produce a higher level of income. The fund may invest in derivative instruments (such as futures, options and swaps) rather than directly in the underlying securities themselves. The derivatives return is linked to movements in the underlying assets. The assets held by the fund may be denominated in a wide variety of currencies. The investment advisor may use foreign exchange and related derivative instruments to hedge or implement currency positions. For full investment objectives and policy details please refer to the fund’s prospectus. Distribution Policy This share class does not pay out a distribution. Any investment income generated will be reinvested. Dealing Day You can buy and sell shares on most working days in Dublin, exceptions to this are more fully described in the fund holiday calendar available from the Administrator.'




```python
api.get_gifs_code()
```




    'GIFS$$$351'




```python
api.get_mpt_benchmark()
```




    ['USTREAS T-Bill Auction Ave 3 Mon', '100']




```python
api.get_primary_benchmark()
```




    ['JPM EMBI Global TR USD',
     '100',
     'JPMorgan Emerging Markets Bond',
     'FOUSA06CYX']




```python
api.get_secondary_benchmark()
```

If we want to switch to another security, just reset the security id:


```python
csdcc = '110011'
secid = mid.from_csdcc_to_secid(csdcc=csdcc)
api.reset_secid(secid=secid)
```


```python
api.get_inception_date()
```




    '2008-06-19'




```python
api.get_manager_list()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>StartDate</th>
      <th>EndDate</th>
      <th>GivenName</th>
      <th>MiddleName</th>
      <th>FamilyName</th>
      <th>CareerStartYear</th>
      <th>College</th>
      <th>Biograhy</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>2008-06-19</td>
      <td>2012-09-28</td>
      <td>Yun Feng</td>
      <td>None</td>
      <td>He</td>
      <td>2004</td>
      <td>n/a</td>
      <td>N/A</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2012-09-28</td>
      <td>NaN</td>
      <td>Kun</td>
      <td>None</td>
      <td>Zhang</td>
      <td>None</td>
      <td>None</td>
      <td>N/A</td>
    </tr>
  </tbody>
</table>
</div>




```python
api.get_investment_obj()
```


```python
api.get_gifs_code()
```


```python
api.get_mpt_benchmark()
```




    ['RMB 3 month Lump-Sum Deposit Rate', '100']




```python
api.get_primary_benchmark()
```




    ['TX Smallcap Yd',
     '35',
     'TX Midcap Yd',
     '45',
     'ChinaBond Aggregate Yd CNY',
     '20',
     'TX Smallcap Yld',
     'F00000US4B',
     'TX Midcap Yld',
     'F00000US4J',
     'ChinaBond Aggregate Yld CNY',
     'F00000USDJ']




```python
api.get_secondary_benchmark()
```


```python

```
