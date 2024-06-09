#!/usr/bin/env python
# |-----------------------------------------------------------------------------
# |            This source code is provided under the Apache 2.0 license      --
# |  and is provided AS IS with no warranty or guarantee of fit for purpose.  --
# |                See the project's LICENSE.md for details.                  --
# |           Copyright Thomson Reuters 2018. All rights reserved.            --
# |-----------------------------------------------------------------------------
"""
Simple example of authenticating to EDP-GW and using the token to query VIPs
from EDP service discovery, login to the Elektron Real-Time Service, and
retrieve MarketPrice content. A username and password are used to
retrieve this token.

document url: https://developers.refinitiv.com/elektron/websocket-api/learning

"""

import sys
import time
import getopt
import requests
import socket
import json
import websocket
import threading

# Global Default Variables
app_id = '256'
auth_hostname = 'api.edp.thomsonreuters.com'
auth_port = '443'
auth_path = 'auth/oauth2/beta1/token'
discovery_path = 'streaming/pricing/v1/'

position = ''
sts_token = ''
refresh_token = ''
user = 'GE-A-01603835-3-1752'
password = 'A2m6GbHKbCV)oLuU'
# user = 'GE-A-01603835-3-1751'
# password = '[zvd@QyN6H6fPwMs'
client_secret = ''
scope = 'trapi'
region = 'apac'
# ric = '/TRI.N'
# ric = '/0700.HK' # slash for delayed data
# ric = 'HMHN9'
ric_list =['0005.HK']
# ric_list =['EUR=','JPY=', 'EURJPY=']
hostList = []
hotstandby = False
# Global Variables
session2 = None


class WebSocketSession:
    logged_in = False
    session_name = ''
    web_socket_app = None
    web_socket_open = False
    host = ''
    disconnected_by_user = False

    def __init__(self, name, host):
        self.session_name = name
        self.host = host

    def _send_market_price_request(self, ric_name):
        """ Create and send simple Market Price request

        "Service":"ELEKTRON_DD" in mp_req_json
        """


        mp_req_json = {
            'ID': 70,
            'Domain': 'MarketPrice',
            "View": [
                "QUOTE_DATE", # orderbook info
                "QUOTIM_MS",
                "BID",
                "ASK",
                "BIDSIZE",
                "ASKSIZE",
                "MID_PRICE",


                "TRADE_DATE", # trade info
                "SALTIM_MS",
                "TRDPRC_1",
                "TRDVOL_1",
                "PRIMACT_1",

                "LOT_SIZE_A", # other info
                "OFF_CLOSE",
                'PROV_SYMB',
                "DSPLY_NAME",
            ],

            # requested a single MarketPrice
            # 'Key': {
            #     'Name': ric_name,
            # },


            # Make a Batch Request for Multiple Items
            "Key": {
                "Name":
                    ric_name,
            }
        }
        self.web_socket_app.send(json.dumps(mp_req_json))
        print("SENT on 00" + self.session_name + ":")
        print(json.dumps(mp_req_json, sort_keys=True, indent=2, separators=(',', ':')))

    def _send_login_request(self, auth_token, is_refresh_token):
        """
            Send login request with authentication token.
            Used both for the initial login and subsequent reissues to update the authentication token

            He we are setting the following:

            ID – Unique identifier for each event stream between your application and server, use value of 1 for the Login request
            Username – often referred to as a ‘DACS’ username (DACS is the authentication and entitlement system used by TREP).
            ApplicationID – value allocated to your application by your organisation, otherwise use default value of 256
            Position – the local IP address / hostname of the PC that your application is running on
        """
        login_json = {
            'ID': 1,
            'Domain': 'Login',
            'Key': {
                'NameType': 'AuthnToken',
                'Elements': {
                    'ApplicationId': '',
                    'Position': '',
                    'AuthenticationToken': ''
                }
            }
        }

        login_json['Key']['Elements']['ApplicationId'] = app_id
        login_json['Key']['Elements']['Position'] = position
        login_json['Key']['Elements']['AuthenticationToken'] = auth_token

        # If the token is a refresh token, this is not our first login attempt.
        if is_refresh_token:
            login_json['Refresh'] = False

        # Once the Login request has been sent, we can expect an asynchronous response from the server in the form of a JSON message over the Websocket.
        self.web_socket_app.send(json.dumps(login_json))
        print("SENT on " + self.session_name + ":")
        print(json.dumps(login_json, sort_keys=True, indent=2, separators=(',', ':')))

    def _process_login_response(self, message_json):
        """ Send item request """
        if message_json['State']['Stream'] != "Open" or message_json['State']['Data'] != "Ok":
            print("Login failed.")
            sys.exit(1)

        # If we do receive a Login Refresh, we are now ready to request some data from the server.
        self.logged_in = True
        print ("loggedddd")
        # self._send_market_price_request(ric)
        self._send_market_price_request(ric_list)

    def _process_message(self, message_json):
        """ Parse at high level and output JSON of message """
        message_type = message_json['Type']

        if message_type == "Refresh":
            # print (message_json)
            if 'Domain' in message_json:
                message_domain = message_json['Domain']
                if message_domain == "Login":
                    self._process_login_response(message_json)
        elif message_type == "Ping":
            # Before we do that, note the ‘Ping’ and ‘Pong’ messages - we are responding to the server’s ‘Ping’ message with a ‘Pong’ message to confirm that we are still alive and running.
            pong_json = {'Type': 'Pong'}
            self.web_socket_app.send(json.dumps(pong_json))
            print("SENT on " + self.session_name + ":")
            print(json.dumps(pong_json, sort_keys=True, indent=2, separators=(',', ':')))

    # Callback events from WebSocketApp
    def _on_message(self, message):
        """ Called when message received, parse message into JSON for processing

        The Initial Refresh message will contain all possible fields available for that RIC – including any currently empty fields.
        The Update message, however, will contain those fields that have changed since the Refresh message or previous Update message.
        """
        print("RECEIVED on " + self.session_name + ":")
        message_json = json.loads(message)
        print(json.dumps(message_json, sort_keys=True, indent=2, separators=(',', ':')))

        for singleMsg in message_json:
            self._process_message(singleMsg)

    def _on_error(self, error):
        """ Called when websocket error has occurred """
        print(error + " for " + self.session_name)

    def _on_close(self):
        """ Called when websocket is closed """
        self.web_socket_open = False
        self.logged_in = False
        print("WebSocket Closed for " + self.session_name)

        if not self.disconnected_by_user:
            print("Reconnect to the endpoint for " + self.session_name + " after 3 seconds... ")
            time.sleep(3)
            self.connect()

    def _on_open(self):
        """ Called when handshake is complete and websocket is open, send login """

        print("WebSocket successfully connected for " + self.session_name + "!")
        # Once the connection to the server has been established and the Websocket is open we should get a callback to our defined method ‘on_open’
        self.web_socket_open = True
        # We can then attempt a login to the server by sending a Login domain JSON message to the server.
        self._send_login_request(sts_token, False)

    # Operations
    # 第一步：Websocket Connection to ADS
    def connect(self):
        # Start websocket handshake
        ws_address = "wss://{}/WebSocket".format(self.host)
        print("Connecting to WebSocket " + ws_address + " for " + self.session_name + "...")
        self.web_socket_app = websocket.WebSocketApp(ws_address, on_message=self._on_message,
                                                     on_error=self._on_error,
                                                     on_close=self._on_close,
                                                     subprotocols=['tr_json2'])
        # Once the websocket is open – on_open
        # When the websocket is closed– on_close
        # If an error occurs – on_error
        # When it receives a message from the ERT in Cloud server – on_message
        self.web_socket_app.on_open = self._on_open

        # Event loop
        wst = threading.Thread(target=self.web_socket_app.run_forever, kwargs={'sslopt': {'check_hostname': False}})
        wst.start()

    def disconnect(self):
        print("Closing the WebSocket connection for " + self.session_name)
        self.disconnected_by_user = True
        if self.web_socket_open:
            self.web_socket_app.close()

    def refresh_token(self):
        if self.logged_in:
            print("Refreshing the access token for " + self.session_name)
            self._send_login_request(sts_token, True)


def query_service_discovery():

    url = 'https://{}/{}'.format(auth_hostname, discovery_path)
    print("Sending EDP-GW service discovery request to " + url)

    try:
        r = requests.get(url, headers={"Authorization": "Bearer " + sts_token}, params={"transport": "websocket"})

    except requests.exceptions.RequestException as e:
        print('EDP-GW service discovery exception failure:', e)
        return False

    if r.status_code != 200:
        print('EDP-GW service discovery result failure:', r.status_code, r.reason)
        print('Text:', r.text)
        return False

    response_json = r.json()
    print("EDP-GW Service discovery succeeded. RECEIVED:")
    print(json.dumps(response_json, sort_keys=True, indent=2, separators=(',', ':')))

    for index in range(len(response_json['services'])):

        if region == "amer":
            if not response_json['services'][index]['location'][0].startswith("us-"):
                continue
        elif region == "emea":
            if not response_json['services'][index]['location'][0].startswith("eu-"):
                continue
        elif region == "apac":
            if not response_json['services'][index]['location'][0].startswith("ap-"):
                continue

        if not hotstandby:
            if len(response_json['services'][index]['location']) == 2:
                hostList.append(response_json['services'][index]['endpoint'] + ":" +
                                str(response_json['services'][index]['port']))
                break
        else:
            if len(response_json['services'][index]['location']) == 1:
                hostList.append(response_json['services'][index]['endpoint'] + ":" +
                                str(response_json['services'][index]['port']))

    if hotstandby:
        if len(hostList) < 2:
            print("hotstandby support requires at least two hosts")
            sys.exit(1)
    else:
        if len(hostList) == 0:
            print("No host found from EDP service discovery")
            sys.exit(1)

    return True


def get_sts_token(current_refresh_token):
    """
        Retrieves an authentication token.
        :param current_refresh_token: Refresh token retrieved from a previous authentication, used to retrieve a
        subsequent access token. If not provided (i.e. on the initial authentication), the password is used.
    """
    url = 'https://{}:{}/{}'.format(auth_hostname, auth_port, auth_path)

    if not current_refresh_token:  # First time through, send password
        data = {'username': user, 'password': password, 'grant_type': 'password', 'takeExclusiveSignOnControl': True,
                'scope': scope}
        print("Sending authentication request with password to ", url, "...")
    else:  # Use the given refresh token
        data = {'username': user, 'refresh_token': current_refresh_token, 'grant_type': 'refresh_token',
                'takeExclusiveSignOnControl': True}
        print("Sending authentication request with refresh token to ", url, "...")

    try:
        r = requests.post(url,
                          headers={'Accept': 'application/json'},
                          data=data,
                          auth=(user, client_secret),
                          verify=True)

    except requests.exceptions.RequestException as e:
        print('EDP-GW authentication exception failure:', e)
        return None, None, None

    if r.status_code != 200:
        print('EDP-GW authentication result failure:', r.status_code, r.reason)
        print('Text:', r.text)
        if r.status_code == 401 and current_refresh_token:
            # Refresh token may have expired. Try using our password.
            return get_sts_token(None)
        return None, None, None

    auth_json = r.json()
    print("EDP-GW Authentication succeeded. RECEIVED:")
    print(json.dumps(auth_json, sort_keys=True, indent=2, separators=(',', ':')))

    return auth_json['access_token'], auth_json['refresh_token'], auth_json['expires_in']


def print_commandline_usage_and_exit(exit_code):
    print('Usage: market_price_edpgw_service_discovery.py [--app_id app_id] '
          '[--user user] [--password password] [--position position] [--auth_hostname auth_hostname] '
          '[--auth_port auth_port] [--scope scope] [--region region] [--ric ric] [--hotstandby]'
          ' [--help]')
    sys.exit(exit_code)


if __name__ == "__main__":
    # Get command line parameters
    opts = []
    try:
        opts, args = getopt.getopt(sys.argv[1:], "", ["help", "app_id=", "user=", "password=",
                                                      "position=", "auth_hostname=", "auth_port=", "scope=",
                                                      "region=", "ric=", "hotstandby"])
    except getopt.GetoptError:
        print_commandline_usage_and_exit(2)
    for opt, arg in opts:
        if opt in "--help":
            print_commandline_usage_and_exit(0)
        elif opt in "--app_id":
            app_id = arg
        elif opt in "--user":
            user = arg
        elif opt in "--password":
            password = arg
        elif opt in "--position":
            position = arg
        elif opt in "--auth_hostname":
            auth_hostname = arg
        elif opt in "--auth_port":
            auth_port = arg
        elif opt in "--scope":
            scope = arg
        elif opt in "--region":
            region = arg
            if region != "amer" and region != "emea":
                print("Unknown region \"" + region + "\". The region must be either \"amer\" or \"emea\".")
                sys.exit(1)
        elif opt in "--ric":
            ric = arg
        elif opt in "--hotstandby":
                hotstandby = True

    if user == '' or password == '':
        print("user and password are required options")
        sys.exit(2)

    if position == '':
        # Populate position if possible
        try:
            position_host = socket.gethostname()
            position = socket.gethostbyname(position_host) + "/" + position_host
        except socket.gaierror:
            position = "127.0.0.1/net"

    sts_token, refresh_token, expire_time = get_sts_token(None)
    if not sts_token:
        sys.exit(1)

    # Query VIPs from EDP service discovery
    if not query_service_discovery():
        print("Failed to retrieve endpoints from EDP Service Discovery. Exiting...")
        sys.exit(1)

    # Start websocket handshake; create two sessions when the hotstandby parameter is specified.
    session1 = WebSocketSession("session1", hostList[0])
    session1.connect()

    if hotstandby:
        session2 = WebSocketSession("session2", hostList[1])
        session2.connect()

    try:
        while True:
            # Give 30 seconds to obtain the new security token and send reissue
            if int(expire_time) > 30:
                time.sleep(int(expire_time) - 30)
            else:
                # Fail the refresh since value too small
                sys.exit(1)
            sts_token, refresh_token, expire_time = get_sts_token(refresh_token)
            if not sts_token:
                sys.exit(1)

            # Update token.
            session1.refresh_token()
            if hotstandby:
                session2.refresh_token()

    except KeyboardInterrupt:
        session1.disconnect()
        if hotstandby:
            session2.disconnect()