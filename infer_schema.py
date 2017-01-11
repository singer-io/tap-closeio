#!/usr/bin/env python3

import json
import sys

past_headers = False

filename = sys.argv[1]

observed_types = {}

def add_observation(path):

    global observed_types
    node = observed_types
    for i in range(0, len(path) - 1):
        if step not in node:
            node[step] = {}
        node = node[step]

    node[path[-1]] = True

def add_observations(path, data):
    if isinstance(data, dict):
        for key in dict:
            add_observations(path + ["object", key], dict[key])
    elif isinstance(data, list):
        for item in data:
            add_observations(path + ["array", key], dict[key])
    elif isinstance(data, basestring):
        add_observation(path + ["string"])

for line in sys.stdin:
    if line == '--\n':
        past_headers = True
        continue

    if past_headers:
        rec = json.loads(line)
        if rec['type'] == 'RECORD':
            
