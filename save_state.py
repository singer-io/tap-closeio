#!/usr/bin/env python3

import json
import sys

past_headers = False

filename = sys.argv[1]

for line in sys.stdin:
    if line == '--\n':
        past_headers = True
        continue

    if past_headers:
        rec = json.loads(line)
        if rec['type'] == 'BOOKMARK':
            print('Saving a bookmark')
            with open(filename, 'w') as out:
                out.write(json.dumps(rec['value']))
            
