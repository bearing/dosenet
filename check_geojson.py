from __future__ import print_function
import json
import ipdb

data = json.load(open('output.geojson', 'r'))
print(json.dumps(data, indent=4, sort_keys=True))

#ipdb.set_trace()
