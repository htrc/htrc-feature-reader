import urllib, urllib2, json

default_params = {
    "wt" : "json",
    "rows" : 2,
    "q" : "test"
        }

url = "http://chinkapin.pti.indiana.edu:9994/solr/ocr/select"

params = urllib.urlencode(default_params);

print url + '?' + params
request = urllib2.Request(url + '?' + params)
response = urllib2.urlopen(request)
rawjson = response.read()
j = json.loads(rawjson)

results = j['response']['docs']

for result in results:
    print result
