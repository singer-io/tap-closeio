#!/usr/bin/env python3

import json
import logging
import logging.config
import os
import sys
import argparse
import re

import requests
import stitchstream as ss
import backoff
import arrow

base_url = 'https://app.close.io/api/v1'

return_limit = 100

default_start_date = '2000-01-01T00:00:00Z'

state = {
    'leads': default_start_date,
    'activities': default_start_date
}

logger = ss.get_logger()

session = requests.Session()

class StitchException(Exception):
    """Used to mark Exceptions that originate within this tap."""
    def __init__(self, message):
        self.message = message

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

def get_leads_custom_fields(auth):
    params = {
        '_limit': return_limit,
        '_skip': 0
    }

    logger.info('Fetching leads custom fields meta data')

    fields = []

    has_more = True
    while has_more:
        logger.info('Fetching leads custom fields with offset ' + str(params['_skip']) +
                    ' and limit ' + str(params['_limit']))
        response = request(url=base_url + '/custom_fields/lead/', params=params, auth=auth)
        body = response.json()
        fields += body['data']
        if len(body['data']) == 0:
            return fields

        has_more = 'has_more' in body and body['has_more']
        params['_skip'] += return_limit

    return fields

def closeio_type_to_json_type(closeio_type):
    if closeio_type == 'datetime' or closeio_type == 'date':
        return {
            'type': ['null', 'string'],
            'format': 'date-time'
        }

    if closeio_type == 'number':
        return {'type': ['null', 'number']}

    return {'type': ['null', 'string']}

def get_leads_schema(auth, lead_schema):
    custom_fields = get_leads_custom_fields(auth)

    properties = {}
    for field in custom_fields:
        properties[field['name']] = closeio_type_to_json_type(field['type'])

    lead_schema['properties']['custom'] = {
        'type': 'object',
        'properties': properties
    }

def normalize_datetime(d):

    if d is None:
        return d
    if not isinstance(d, str):
        raise Exception('Expected string but got {} of type {}'
                        .format(d, type(d)))

    try:
        return arrow.get(d).isoformat()
    except arrow.parser.ParserError:
        pass
    # Some of CloseIO's dates are like Mon, Dec 12, 2015 at 2:15 PM. We
    # need to remove the " at " for arrow to be able to parse it.
    d = d.replace(' at ', ' ')
    for fmt in ['D MMM YYYY HH:mm:ss Z',
                'ddd, MMM D, YYYY h:m A']:
        try:
            return arrow.get(d, fmt).isoformat()
        except arrow.parser.ParserError:
            pass
    raise Exception('Unrecognized date/time value ' + d)

def get_contacts(auth, partial_contacts):
    contacts = []
    for partial_contact in partial_contacts:
        response = request(url=base_url + '/contact/' + partial_contact['id'] + '/', auth=auth)
        contacts.append(response.json())
    return contacts

def normalize_lead(lead, lead_schema):
    custom_field_schema = lead_schema['properties']['custom']

    if 'tasks' in lead:
        for task in lead['tasks']:
            for k in ['date', 'due_date']:
                if k in task:
                    task[k] = normalize_datetime(task[k])

    if 'opportunities' in lead:
        for op in lead['opportunities']:
            if 'date_won' in op:
                op['date_won'] = normalize_datetime(op['date_won'])

    if 'custom' in lead:
        custom = lead['custom']
        for prop in custom_field_schema['properties']:
            if prop in custom:
                field = custom_field_schema['properties'][prop]
                if 'format' in field and field['format'] == 'date-time':
                    lead['custom'][prop] = normalize_datetime(custom[prop])

def get_leads(auth, lead_schema):
    global state

    params = {
        '_limit': return_limit,
        '_skip': 0,
        'query': 'date_updated >= ' + state['leads'] + ' sort:date_updated'
    }

    logger.info("Fetching leads starting at " + state['leads'])

    count = 0

    has_more = True
    while has_more:
        logger.info("Fetching leads with offset " + str(params['_skip']) +
                    " and limit " + str(params['_limit']))
        response = request(url=base_url + '/lead/', params=params, auth=auth)
        body = response.json()
        data = body['data']

        if len(data) == 0:
            return

        count += len(data)
        logger.info("Fetched " + str(count) + " leads in total")
        
        for lead in data:
            normalize_lead(lead, lead_schema)
            lead['contacts'] = get_contacts(auth, lead['contacts'])

        ss.write_records('leads', data)
        state['leads'] = data[-1]['date_updated']
        ss.write_state(state)

        has_more = 'has_more' in body and body['has_more']
        params['_skip'] += return_limit


def normalize_activity(activity):
    if 'envelope' in activity and 'date' in activity['envelope']:
        activity['envelope']['date'] = normalize_datetime(activity['envelope']['date'])
    if 'date_scheduled' in activity:
        activity['date_scheduled'] = normalize_datetime(activity['date_scheduled'])
    if 'send_attempts' in activity:
        for attempt in activity['send_attempts']:
            if 'date' in attempt:
                attempt['date'] = normalize_datetime(attempt['date'])

def get_activities(auth):
    global state

    params = {
        '_limit': return_limit,
        '_skip': 0,
        'date_created__gt': state['activities']
    }

    logger.info("Fetching activities starting at " + state['activities'])

    count = 0

    if 'activities' in state:
        last_date_created = state['activities']
    
    has_more = True
    while has_more:
        logger.info("Fetching activities with offset " + str(params['_skip']) +
                    " and limit " + str(params['_limit']))
        response = request(url = base_url + '/activity/', params=params, auth=auth)
        body = response.json()

        data = body['data']

        if len(data) == 0:
            break

        for activity in data:
            normalize_activity(activity)
            if (last_date_created is None or
                    activity['date_created'] > last_date_created):
                last_date_created = activity['date_created']

        count += len(data)
        logger.info("Fetched " + str(count) + " activities in total")

        ss.write_records('activities', data)
        
        has_more = 'has_more' in body and body['has_more']
        params['_skip'] += return_limit

    state['activities'] = last_date_created
    ss.write_state(state)

def do_check(args):
    with open(args.config) as file:
        config = json.load(file)

    auth = (config['api_key'],'')

    params = {
        '_limit': 10
    }

    try:
        request(url=base_url + '/lead/', params=params, auth=auth)
    except requests.exceptions.RequestException as e:
        logger.fatal("Error checking connection using " + e.request.url +
                     "; received status " + str(e.response.status_code) +
                     ": " + e.response.text)
        sys.exit(-1)

def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

def load_schemas(auth):
    schemas = {}

    with open(get_abs_path('tap_closeio/leads.json')) as file:
        schemas['leads'] = json.load(file)

    get_leads_schema(auth, schemas['leads'])

    with open(get_abs_path('tap_closeio/activities.json')) as file:
        schemas['activities'] = json.load(file)

    return schemas

def do_sync(args):
    global state
    with open(args.config) as file:
        config = json.load(file)

    if args.state != None:
        logger.info("Loading state from " + args.state)
        with open(args.state) as file:
            state_arg = json.load(file)
        for key in ['leads', 'activities']:
            if key in state_arg:
                state[key] = state_arg[key]

    logger.info('Replicating all Close.io data, with starting state ' + repr(state))

    auth = (config['api_key'],'')

    schemas = load_schemas(auth)
    ss.write_schema('leads', schemas['leads'], 'id')
    ss.write_schema('activities', schemas['activities'], 'id')

    try:
        get_leads(auth, schemas['leads'])
        get_activities(auth)
        logger.info("Tap exiting normally")
    except requests.exceptions.RequestException as e:
        logger.fatal("Error on " + e.request.url +
                     "; received status " + str(e.response.status_code) +
                     ": " + e.response.text)
        sys.exit(-1)


def main():
    global logger
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '-c', '--config', help='Config file', required=True)
    parser.add_argument(
        '-s', '--state', help='State file')

    args = parser.parse_args()

    do_sync(args)


if __name__ == '__main__':
    main()
