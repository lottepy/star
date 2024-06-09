import telnetlib

# auto switch network
def network_utils(main_host='172.28.5.240', main_port='80',
                  backup_host='', backup_port='443'):
	HOST = None
	PORT = None
	EP = None

	try:
		conn = telnetlib.Telnet(main_host, main_port, timeout=2)
		conn.close()
		HOST=main_host
		PORT=main_port
	except:
		try:
			conn = telnetlib.Telnet(backup_host, backup_port, timeout=2)
			conn.close()
			HOST = backup_host
			PORT = backup_port
		except:
			raise ConnectionError("network error")
	if str(PORT) == '443':
		EP = 'https://'+f"{HOST}:{PORT}"
	else:
		EP = 'http://'+f"{HOST}:{PORT}"
	return EP,HOST,PORT

# MARKET_EP,_,_ = network_utils('10.2.3.4', '8003','market.aqumon.com','443') # Market by AQM
MARKET_EP,_,_ = network_utils('market-internal.aqumon.com', '80','market.aqumon.com','443') # Market by AQM
ALGO_EP,_,_ = network_utils('market-internal.aqumon.com', '80','market.aqumon.com','443') # Market by AQM
C_EP, C_HOST, C_PORT = network_utils('172.19.247.50', '8394','172.19.193.89','8394') # Choice
B_EP, B_HOST, B_PORT = network_utils('172.31.86.16', '18196','172.31.86.16', '18194') # Bloomberg

