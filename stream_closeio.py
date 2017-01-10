#!/usr/bin/env python3

import json
import logging
import os
import sys
import argparse

import requests
import stitchstream as ss
import backoff

logger = logging.getLogger()

base_url = 'https://app.close.io/api/v1'
return_limit = 100

auth = (os.environ['STITCH_CLOSEIO_API_KEY'],'')
print(auth)
session = requests.Session()

class StitchException(Exception):
    def __init__(self, message):
        self.message = message

def configure_logging(level=logging.DEBUG):
    global logger
    logger.setLevel(level)
    ch = logging.StreamHandler()
    ch.setLevel(level)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)

def client_error(e):
    return e.response is not None and 400 <= e.response.status_code < 500

@backoff.on_exception(backoff.expo,
                      (requests.exceptions.RequestException),
                      max_tries=5,
                      giveup=client_error,
                      factor=2)
def request(**kwargs):
    if 'method' not in kwargs:
        kwargs['method'] = 'get'

    response = session.request(**kwargs)
    response.raise_for_status()
    return response

def get_leads(start_date, offset=0):
    params = {
        '_limit': return_limit,
        '_skip': offset,
        'query': 'date_updated >= ' + start_date + ' sort:date_updated'
    }
    
    response = request(url=base_url + '/lead/', params=params, auth=auth)

    print(response.json())
    
def main():
    parser = argparse.ArgumentParser(prog='Close.io Streamer')
    args = parser.parse_args()

    configure_logging()

    logger.info('Replicating all Close.io data')

    ## TODO: write schemas to stream

    try:
        get_leads('2017-01-01T00:00:00Z')
    except requests.exceptions.RequestException as e:
        logger.fatal("Error on " + e.request.url +
                     "; received status " + str(e.response.status_code) +
                     ": " + e.response.text)
        sys.exit(-1)
    
if __name__ == '__main__':
    main()
