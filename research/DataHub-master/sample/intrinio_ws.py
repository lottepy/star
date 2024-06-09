from intriniorealtime.client import IntrinioRealtimeClient


def on_quote(quote, backlog):
    print("QUOTE: ", quote, "BACKLOG LENGTH: ", backlog)


options = {
    'username': 'fd89f36635604f0e0810f0bef2707b99',
    'password': 'a61509770c62a492ed5be350c7f51381',
    'provider': 'quodd',
    'on_quote': on_quote
}

client = IntrinioRealtimeClient(options)
client.join(['AAPL.NB', 'GE.NB', 'MSFT.NB'])
client.connect()
client.keep_alive()