import json
import requests
from elasticsearch import Elasticsearch
import sys
# from elasticsearch_dsl import DocType, Date, Integer, Keyword, Text
# from elasticsearch_dsl.connections import connections

es = Elasticsearch([{'host': 'localhost', 'port': 9200}])


# url = "https://gist.githubusercontent.com/slaterb1/0f59922d2f68b4cbbbaa088089bdc41f/raw/67eab0985cf6efec2811874eda6e83456b3c01b9/git-events.log"
# Below intended for testing purposes as it reads from a static file to make development faster.
# Needs to be reformatted to import data from above url for production.
# On second thought should be source agnostic ideally, so it can be repurposed easily for injesting live data.

# GET_JSON_STATIC - Imports a limited version of log events file from a local static file.

def get_json_static():
	data = open("static-data.json")
	c = data.readlines()
	return c


# GENERATE_DATA_MODEL - Reformats each event and extracts relevant data to form a new JSON object.

def generate_data_model(log_event):
	event = json.loads(log_event)
	data_model = {
		"team" : event.get("org").get("login"), # "repo":{"name":"C/proj2","href":"http://git-test/C/proj2"}
		"repo" : event.get("repo").get("name").split("/")[1], # "org":{"login":"C","href":"http://git-test/C/","avatar":"http://git-test/C/pic"}
		"user" : event.get("payload").get("user").get("login"), # "payload": {"user": {"login" : "test" ...},...}
		"event": {
			"type" : event.get("type"), # "type":"issue"
			"action" : event.get("payload").get("action"), #"payload":{"action":"created",...}
		}, 
		"time" : event.get("created_at"), # "created_at":"2017-09-19T19:39:22.368Z"
	}

	return data_model


# CLEAN - Brings the above two functions together to create data models for each line in file.

def clean():
	events = []
	log_events = get_json_static()
	for event in log_events:
		data = generate_data_model(event)
		events.append(data)
	return events


def index_into_es(data):
	total = 0
	success = 0 
	fail = 0
	fail_reasons = []

	for d in data:
		total += 1
		print("clean.py line 59 ------ index_into_es, Inserting in ES - Event: {}; Action: {}; Time: {};".format(d.get("event").get("type"), d.get("event").get("action"), d.get("time")))
		response = es.index(index='static', doc_type='events', body=d)
		print("clean.py line 62 ------ index_into_es, ID: {}".format(response.get("_id")))
		if response.get("result") == "created":
			success += 1
		else: 
			fail += 1
			fail_reasons.append(response)

	message = "clean.py line 70 ------ index_into_es, {} of {} successfully inserted. {} Failed. Reasons for failure are as follows: \n\n {}".format(success, total, fail, fail_reasons)
	return message


def main():
	script_result = index_into_es(clean())
	print("clean.py line 60 ------ {}".format(script_result))
	sys.stdout.flush()


#start process
if __name__ == '__main__':
    main()












