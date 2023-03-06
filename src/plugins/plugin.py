#!/usr/bin/python3
import json
import sys

record = json.loads(sys.argv[1])
record['Reference'] = "301"
print(json.dumps(record))
