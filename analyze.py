import json
import requests
import numpy
import datetime
from elasticsearch import Elasticsearch
from addict import Dict #Fuck yes: http://blog.comperiosearch.com/blog/2014/12/17/crafting-elasticsearch-queries-python-hassle-free-way/
from create_es_queries import q_teams_with_data, q_active_repos_by_team, q_doc_by_id, q_general_query, q_general_query_limit
es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

# STILL NEED TO CODE CALCULATE_TIME_SUM_DIFFS()!!!
# STILL NEED TO CODE CALCULATE_TIME_SUM_DIFFS()!!!
# STILL NEED TO CODE CALCULATE_TIME_SUM_DIFFS()!!!
# STILL NEED TO CODE CALCULATE_TIME_SUM_DIFFS()!!!
# STILL NEED TO CODE CALCULATE_TIME_SUM_DIFFS()!!!

# =========== CODE NOT VERIFIED AT ALL NEEDS TO BE TESTED ======================================================

# RETURNS COUNT OF EVENTS GROUPED BY TEAM.

def teams_with_data(index, doc_type):
	team_event_count = {}
	body = q_teams_with_data()
	response = es.search(index = index, doc_type = doc_type, body = body)
	response_buckets = response.get("aggregations").get("teams").get("buckets")
	for element in response_buckets:
		team_event_count[element.get("key")] = element.get("doc_count")
	return team_event_count


# RETURNS COUNT OF EVENTS PER ACTIVE REPO BY TEAM, DEFINITION FOR ACTIVE IS FLEXIBLE, DEFAULT 14 DAYS.

def active_repos_by_team(index, doc_type, team, days=14): # Days are set to 2 weeks by default, can be overriden by passing value
	team_active_repos = Dict()
	team_active_repos.team = team
	team_active_repos.active_threshold_days = days
	team_active_repos.active_repos = {}
	body = q_active_repos_by_team(team, days)
	response = es.search(index = index, doc_type = doc_type, body = body)
	response_buckets = response.get("aggregations").get("repos").get("buckets")
	for element in response_buckets:
		team_active_repos.active_repos[element.get("key")] = {"events": element.get("doc_count")}
	return team_active_repos


# RETURNS FREQUENCY OF ANY COMBINATION OF TERM-ABLE QUERY, PASSED IN PARAMS.

def get_frequency(index, doc_type, params):
	average = Dict()
	key = index + doc_type + params.get("team") + params.get("repo") + params.get("event_type") + params.get("event_action")
	last_body = q_doc_by_id([key])
	last_average = es.search(index = "avglogs", doc_type = "avg", body = last_body).get("hits")

	if last_average.get("total") != 0:
		date_min = last_average.get("hits").get("_source").get("timestamp")
		date_max = datetime.now()
		new_body = q_events_date_range(params, date_min, date_max)
		new_events = es.search(index = "avglogs", doc_type = "avg", body = new_body).get("hits")
		avg = update_frequency(index, doc_type, new_events, last_average)
		average["key"] = key
		average["average"] = avg["average"]
		average["count"] = avg["count"]
		average["timestamp"] = datetime.now()

	else:
		avg = new_frequency(index, doc_type, params)
		average["key"] = key
		average["average"] = avg.get("average")
		average["count"] = avg.get("count")
		average["timestamp"] = datetime.now()

	response = es.index(index='avglogs', doc_type='avg', body=average)
	print(response)
	return average


# GENERATES THE FREQUENCY OF ANY COMBINATION OF TERM-ABLE QUERY, PASSED IN PARAMS, SAMPLES ALL EVENTS.

def new_frequency(index, doc_type, params):
	avg = Dict()
	key = index + doc_type + team + repo + event_type + event_action
	body = q_general_query_limit(params)
	events = es.search(index = "avglogs", doc_type = "avg", body = body).get("hits")
	sum_diffs = calculate_time_sum_diffs(events)
	avg.count = events.get("total")
	avg.average = (sum_diffs / average["count"])
	print(avg)
	return avg


# GENERATES THE FREQUENCY OF ANY COMBINATION OF TERM-ABLE QUERY, PASSED IN PARAMS, SAMPLES ONLY NEW EVENTS.

def update_frequency(index, doc_type, new_events, last_average):
	avg = Dict()
	count_new = new_events.get("total")
	sum_diffs_new = calculate_time_sum_diffs(new_events)
	average_old = last_average.get("hits").get("_source").get("average")
	count_old = last_average.get("hits").get("_source").get("count")
	avg.count = count_new + count_old
	avg.average = (((average_old * count_old) + sum_diffs_new) / (avg["count"]))
	print(avg)
	return avg


# === !! NOT FINISHED !! === CALCULATES THE TIME DIFFERENCE BETWEEN EACH SEQUENTIAL EVENT PASSED TO IT IN AN ARRAY.

def calculate_time_sum_diffs(events):
	timestamps = []
	for event in new_events:
		time = event.get("hits").get("_source").get("time")
		new_events_timestamps.append(time)

	print() #placeholder
	# Order timestamps in array
	# Take first two and calculate time between them, store in another array of diffs
	# Take next two and calculate time between them, store in same array of diffs, repeat
	# Calculate average of second array of diffs
	# Return average


# GETS FREQUENCY OF ALL EVENTS.

def freq_events(index, doc_type):
	params = {}
	calculate_frequency(index, doc_type, params)
	print() #placeholder


# GETS FREQUENCY OF EVENTS BY TYPE.

def freq_events_type(index, doc_type, event_type):
	params = {}
	params["event_type"] = event_type
	calculate_frequency(index, doc_type, params)
	print() #placeholder


# GETS FREQUENCY OF ALL EVENTS BY TEAM.

def freq_events_team(index, doc_type, team):
	params = {}
	params["team"] = team
	calculate_frequency(index, doc_type, params)
	print() #placeholder


# GETS FREQUENCY OF EVENTS BY TYPE AND TEAM.

def freq_events_type_team(index, doc_type, event_type, team):
	params = {}
	params["event_type"] = event_type
	params["team"] = team
	calculate_frequency(index, doc_type, params)
	print() #placeholder


# GETS FREQUENCY OF EVENTS BY TEAM AND REPO.

def freq_events_team_repo(index, doc_type, team, repo):
	params = {}
	params["team"] = team
	params["repo"] = repo
	calculate_frequency(index, doc_type, params)
	print() #placeholder


# GETS FREQUENCY OF EVENTS BY TYPE, TEAM, AND REPO.

def freq_events_type_team_repo(index, doc_type, event_type, team, repo):
	params = {}
	params["event_type"] = event_type
	params["team"] = team
	params["repo"] = repo
	calculate_frequency(index, doc_type, params)
	print() #placeholder 




	# Update repo field to not be analyzed
	# Test query to filter by team, action over certain date, and aggregate by repo

# total number of teams with data [=== DONE ===]
# number of active repos in each team [=== DONE ===]
# total number of events captured overall [=== DONE ===]
# total number of events captured per team [=== DONE ===]
# frequency of event types captured overall
# frequency of event types captured per team = # avg time between any captured events per team
# frequency of event types captured per repo
# avg time between any captured events per team
# avg time between same captured events per team

	# ^ freq & average are same, can use same (avg*count + new value)/(count+1) strategy


# Weaknesses:

	# 1. Researched how to capture a stream of data using Node.js too long
	# 2. Tried to set up Kibana for too long, should've kept it simple and just used console output

# To Do:
	
	# 1. Change mapping to not analyze fields like team and repo and turn them into keywords
	# 2. Figure out how to consume a stream of data using Node.js
	# 3. Check if the Python code is able to consume stream in the same way as static given how it's coded now
	# 4. Write Elastic Search queries for each metric









# print(teams_with_data("static", "events"))
# print(active_repos_by_team("static", "events", "B"))
print(calculate_frequency("static", "events"))







