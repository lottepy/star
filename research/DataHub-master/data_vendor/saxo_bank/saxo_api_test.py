# docs: https://github.com/hootnot/saxo_openapi#saxo-openapi
# https://www.developer.saxo/openapi/learn/plain-websocket-streaming

from saxo_openapi import API
import json
import saxo_openapi.endpoints.rootservices as rs
import saxo_openapi.endpoints.referencedata as rd
import saxo_openapi.endpoints.trading as tr
import saxo_openapi.endpoints.chart as ct
from pprint import pprint

token = "eyJhbGciOiJFUzI1NiIsIng1dCI6IkQ2QzA2MDAwMDcxNENDQTI5QkYxQTUyMzhDRUY1NkNENjRBMzExMTcifQ.eyJvYWEiOiI3Nzc3NyIsImlzcyI6Im9hIiwiYWlkIjoiMTA5IiwidWlkIjoiRFozT1pZTktqR014VVRYaXp2VjBWUT09IiwiY2lkIjoiRFozT1pZTktqR014VVRYaXp2VjBWUT09IiwiaXNhIjoiRmFsc2UiLCJ0aWQiOiIyMDAyIiwic2lkIjoiYWQwY2VhOGZkNzhkNGY3NzlkYTM4M2NmN2FkZTQ1MGMiLCJkZ2kiOiI4NCIsImV4cCI6IjE1NzM3ODQyMTkifQ.-H1kNq1y_1kvZ0Q5A7mUIJkfgHUwbyP8PcQ8BJGtQgnKD4wOt90QhXWYqWTMKhs5gUthKsiVxlnAAMHYaJ97Jg"
client = API(access_token=token)


# # get chart data / historical data
# params = dict(
# AssetType = 'FxSpot',
# Uic = '21',
#     Horizon = '1', # 1 mint
# Mode = 'From',
# Time = '2019-05-17T00:00:00.000000Z',
# Count = '100'
# )
# r = ct.charts.GetChartData(params)
# client.request(r)
# print(json.dumps(r.response, indent=4))
#
# # get currency list
#
# r = rd.currencies.Currencies()
# client.request(r)
# print(json.dumps(r.response, indent=4))
#
# # search instruments
# params = dict(
# AssetTypes = 'FxForwards',
# Keywords = 'TWD'
# )
# r =  rd.instruments.Instruments(params)
# client.request(r)
# print(json.dumps(r.response, indent=4))

# # get info prices, bid and ask
# params = dict(Uics= '22,21', AssetType='FxSpot')
# r = tr.infoprices.InfoPrices(params)
#
# client.request(r)
# print(json.dumps(r.response, indent=4))

# subscribe info price
# r = tr.infoprices.CreateInfoPriceSubscription(data)
data = {
	"Arguments": {
		"Uics": 22,
		"AssetType": "FxSpot"
	},
	"ContextId": "explorer_1573702165343",
	"ReferenceId": "Y_466"
}
r = tr.infoprices.CreateInfoPriceSubscription(data)
# r = tr.prices.PriceSubscriptionRemove('explorer_1573702165343','Y_466')
client.request(r)
print(json.dumps(r.response, indent=4))


# subscribe single price
data = {
	"Arguments": {
		"Uic": 21,
		"AssetType": "FxSpot"
	},
	"ContextId": "explorer_1573702165343",
	"ReferenceId": "Y_465"
}
# r = tr.prices.CreatePriceSubscription(data)
r = tr.prices.PriceSubscriptionRemove('explorer_1573702165343','Y_465')
client.request(r)
print(json.dumps(r.response, indent=4))



# get instrument details
Uic = 22
AssetType = 'FxSpot'
# params = {_v3_InstrumentDetails_params}
r = rd.instruments.InstrumentDetails(Uic=Uic,AssetType = AssetType,
                                     # params = params
                                     )
client.request(r)
print(json.dumps(r.response, indent=4))





# lets make a diagnostics request, it should return '' with a state 200
r = rs.diagnostics.Get()
print("request is: ", r)
rv = client.request(r)
assert rv is None and r.status_code == 200
print('diagnostics passed')

# request available rootservices-features
r = rs.features.Availability()
rv = client.request(r)
print("request is: ", r)
print("response: ")
pprint(rv, indent=2)
print(r.status_code)