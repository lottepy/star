1. search ric code by equity search (for equities), or instrument search
2. import ric list into dss web 
3. use "Reuters Editorial RIC" as the real RIC code
4. if duplicate, use RIC

---
for hk future:
select no digit prefix,
such as HSIK9, HMHK9

for us futures:
select no TAS, such as VXK9



## Note:

1. 在中国大陆地区，并不能使用 "https://hosted.datascopeapi.reuters.com/RestApi/v1/Authentication/RequestToken" 而应该直接使用 ip address   "https://192.165.219.152/RestApi/v1/Authentication/RequestToken"
2. 提取数据时候，应该使用 Extractions/ExtractWithNotes 而不是 Extractions/ExtractRaw, 目的是直接从 URL 获取数据/存储/打印，而不是将返回文件保存成 csv 文件后进行解析再打印。