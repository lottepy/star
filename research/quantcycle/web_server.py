import argparse
import os

from tornado.httpserver import HTTPServer
from tornado.ioloop import IOLoop
from tornado.options import define, options
from tornado.web import Application, url
from tornado_swagger.setup import setup_swagger

from quantcycle.utils.production_helper import (Configuration, DingDingMessage,
                                                LarkMessage, MessageBox)
from views import (Engine_manager_View, Initiation_View, Result_View,
                   Strategy_View, Subscription_View, start_engine_manager,init)

#define('port', default = 1234, help='port to listen on')
parser = argparse.ArgumentParser()
parser.add_argument("--port", help="port for prod server", type=int)
parser.add_argument("--config_path", help="config_path for prod server", type=str)
args = parser.parse_args()

def main(port=1234):
    """Construct and serve the tornado application."""
    _routes = [
                url('/', Initiation_View, name="Initiation"),
                url('/api/strategy_subscription', Subscription_View, name="Subscription"),
                url('/api/engine_manager_control', Engine_manager_View, name="Engine_manager"),
                url('/api/result', Result_View, name="Result"),
                url('/api/strategy', Strategy_View, name="Strategy")
            ]

    setup_swagger(_routes)
    app = Application(_routes)
    http_server = HTTPServer(app)
    http_server.listen(port)
    print(f'Application run Listening on post:{port}')
    IOLoop.current().start()

if __name__ == "__main__":
    if args.config_path:
        Configuration.read(args.config_path)
    MessageBox.update_name(name = Configuration.config["info"]["name"]+":"+str(args.port))
    ddmsg = DingDingMessage(dingding_addr=Configuration.config["info"]["dingding_addr"])
    larkmsg = LarkMessage(webhook_url=Configuration.config["info"]["webhook_url"])
    #MessageBox.add_method(f=ddmsg.send_msg)
    MessageBox.add_method(f=larkmsg.send_msg)
    MessageBox.update_status(is_send_msg = Configuration.config["info"].getboolean("is_send_msg"))
    MessageBox.send_msg("MessageBox init")
    start_engine_manager()
    init()
    main(port=args.port)
