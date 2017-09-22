import json
import requests
import numpy
import datetime
from elasticsearch import Elasticsearch
es = Elasticsearch([{'host': 'localhost', 'port': 9200}])


#response = es.index(index='static', doc_type='events', body=d)

def teams_with_data(index, doc_type):
	team_event_count = {}
	query_body = {
		"size" : 0,
		"aggs" : { "teams" : { "terms" : {  "field" : "team.keyword" } } }
	}
	response = es.search(index = index, doc_type = doc_type, body = query_body)
	response_buckets = response.get("aggregations").get("teams").get("buckets")
	for element in response_buckets:
		team_event_count[element.get("key")] = element.get("doc_count")

	return team_event_count

	# Change mapping to not analyze fields like team and repo and turn them into keywords [=== DONE ===]
	# With that the above code should work and be usable [=== DONE ===]

def active_repos_by_team(index, doc_type, team):
	active_repos = 0
	query_body = {
		"size" : 0,
		"aggs" : { "repos" : {
						"filter" : { "term": { "team": "B" } },
						"terms" : {  "field" : "team.keyword" } } }
	}

def calculate_frequency(index, doc_type, team=None, repo=None, event_type=None):
	print() #placeholder

	# search ES for key
	# if search is True:
		# if count of ES record is equal to current count -1:
			# frequency = (avg*count + new value)/(count+1) strategy
		# else:
			# calculate new frequency with full sample
			# new_frequency(x, y, z)
			# insert new frequency into ES es.index(index='static', doc_type='events', body=d)
	# else:
		# calculate new frequency with full sample
		# new_frequency(x, y, z)
		# insert new frequency into ES es.index(index='static', doc_type='events', body=d)

def new_frequency(index, doc_type, team=None, repo=None, event_type=None):
	print() #placeholder
	# Get dates of all events in the result of this query
	# Put them all in an ordered array
	# Take first two and calculate time between them, store in another array of diffs
	# Take next two and calculate time between them, store in same array of diffs, repeat
	# Calculate average of second array of diffs
	# Return average



	# Update repo field to not be analyzed
	# Test query to filter by team, action over certain date, and aggregate by repo

# total number of teams with data [=== DONE ===]
# number of active repos in each team
# total number of events captured overall [=== DONE ===]
# total number of events captured per team [=== DONE ===]
# frequency of event types captured overall
# frequency of event types captured per team
# frequency of event types captured per repo

	# ^ These are the same as the average, can use same (avg*count + new value)/(count+1) strategy

# avg time between any captured events per team
# avg time between same captured events per team

	# ^ These are the same as the average, can use same (avg*count + new value)/(count+1) strategy


# Weaknesses:

	# 1. Researched how to capture a stream of data using Node.js too long
	# 2. Tried to set up Kibana for too long, should've kept it simple and just used console output

# To Do:
	
	# 1. Change mapping to not analyze fields like team and repo and turn them into keywords
	# 2. Figure out how to consume a stream of data using Node.js
	# 3. Check if the Python code is able to consume stream in the same way as static given how it's coded now
	# 4. Write Elastic Search queries for each metric









teams_with_data("static", "events")







