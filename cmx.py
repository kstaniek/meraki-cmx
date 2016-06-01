#!/usr/bin/env python

import os
import re
import sys
import json
import yaml
import time
import signal
import logging
import threading
import SocketServer
import SimpleHTTPServer

from influxdb import InfluxDBClient
from urlparse import urlparse


_scriptname = os.path.splitext(os.path.basename(__file__))[0]
_config_file = os.getenv(_scriptname.upper() + 'CFG', _scriptname + '.yaml')
_log_file = os.getenv(_scriptname.upper() + 'LOG', _scriptname + '.log')
_log_format = '%(asctime)-15s %(levelname)-5s [%(module)s] %(message)s'


class Config(object):
    def __init__(self, config_file_name):
        with open(config_file_name, 'r') as ymlfile:
            config = yaml.load(ymlfile)

        self.log_level = config.get('loglevel', 'INFO')
        numeric_level = getattr(logging, self.log_level.upper(), None)
        if not isinstance(numeric_level, int):
            raise ValueError('Invalid log level: {}'.format(self.log_level))

        logging.basicConfig(filename=_log_file, level=numeric_level, format=_log_format)
        # logging.basicConfig(level=numeric_level, format=_log_format)
        logging.info("Reading config file: {}".format(_config_file))

        # Meraki CMX config
        parse_result = urlparse(config['cmx']['posturl'])
        self.path = parse_result.path
        self.port = parse_result.port

        self.validator = config['cmx']['validator']
        self.secret = config['cmx']['secret']

        # DB Config
        self.influxdb_url = config['influxdb']['url']

        devices = config['devices']

        self.devices = {}
        for device in devices:
            self.devices[device['mac'].lower()] = {'name': device['name'], 'owner': device['owner']}


try:
    cf = Config(_config_file)
except Exception as e:
    logging.critical("Cannot open configuration at %s: %s" % (_config_file, str(e)))
    sys.exit(2)

logging.info("Log level INFO")
logging.debug('Log level DEBUG')


class Handler(SimpleHTTPServer.SimpleHTTPRequestHandler):
    allow_reuse_address = True

    def log_message(self, format, *args):
        logging.debug(format, *args)

    def do_GET(self):
        if re.search(cf.path, self.path):
            self.send_response(200)
            self.send_header('Content-type', 'text/html')
            self.end_headers()
            # Send the html message
            self.wfile.write(cf.validator)
        else:
            logging.warning("Wrong url: {}".format(self.path))
            self.send_response(404)
            self.end_headers()
        return


    def do_POST(self):
        if not re.search(cf.path, self.path):
            self.send_response(404)
            self.end_headers()
            return

        content_type = self.headers.getheader('content-type', None)
        if content_type != "application/json":
            self.send_response(406)
            self.end_headers()
            logging.warning("Wrong content type: {}".content_type)
            return

        content_len = int(self.headers.getheader('content-length', 0))

        try:
            post_body = self.rfile.read(content_len)
            data = json.loads(post_body)

            if data['secret'] == cf.secret and data['version'] == '2.0':
                if data['type'] == "DevicesSeen":
                    self.devices_seen(data['data'])
                self.send_response(200)

        except Exception as e:
            print(str(e))
            self.send_response(406)
            self.end_headers()
            logging.error("Data error: {}".format(post_body))

        self.end_headers()

    def devices_seen(self, data):
        observations = []
        for ob in data['observations']:
            if ob['ipv4']:
                ob['ipv4'] = ob['ipv4'].lstrip('/')

            device = cf.devices.get(ob['clientMac'], None)
            if device:
                owner, name = device['owner'], device['name']
            else:
                owner, name = None, None

            point_value = {
                "measurement": "observation",
                "tags": {
                    "apMac": data['apMac'],
                    "apFloors": " ".join(data['apFloors']).strip(),
                    "apTags": " ".join(data['apTags']).strip(),
                    "clientMac": ob['clientMac'],
                    "manufacturer": ob['manufacturer'],
                    "os": ob['os'],
                    "ssid": ob['ssid'],
                    "ipv4": ob['ipv4'],
                    "ipv6": ob['ipv6'],
                    "location": ob['location'],
                    "name": name,
                    "owner": owner
                },
                "fields": {
                    "value": ob['rssi'],
                },
                "time": ob['seenTime']
            }
            observations.append(point_value)

        try:
            client.write_points(observations)
            logging.debug("{} observations stored in DB".format(len(observations)))
        except Exception as e:
            logging.warning("Database error: {}".format(str(e)))


def shutdown_handler(event):
    httpd.shutdown()
    event.set()

def terminate(sig, frame):
    sig2name = dict((k, v) for v, k in reversed(sorted(signal.__dict__.items()))
         if v.startswith('SIG') and not v.startswith('SIG_'))

    logging.debug("Terminating the process: {}".format(sig2name[sig]))
    th = threading.Thread(target=shutdown_handler, args=(done_event,))
    th.start()


if __name__ == '__main__':
    client = InfluxDBClient.from_DSN(cf.influxdb_url)
    while True:
        try:
            client.create_database(client._database, if_not_exists=True)
            break

        except Exception as e:
            logging.critical("Error connecting to the database. Retrying in 60s: {}".format(str(e)))

        time.sleep(60)

    done_event = threading.Event()
    httpd = SocketServer.TCPServer(("", cf.port), Handler)

    signal.signal(signal.SIGTERM, terminate)
    signal.signal(signal.SIGINT, terminate)

    logging.info("Starting {} server".format(_scriptname))
    httpd.serve_forever()
    logging.info("Stopped {} server".format(_scriptname))
    done_event.wait()