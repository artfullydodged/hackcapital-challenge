import json
import requests
import numpy
import datetime
from elasticsearch import Elasticsearch
from addict import Dict #Fuck yes: http://blog.comperiosearch.com/blog/2014/12/17/crafting-elasticsearch-queries-python-hassle-free-way/
from create_es_queries import q_teams_with_data, q_active_repos_by_team, q_doc_by_id, q_general_query, q_events_date_range
import dateparser

# TIP: FIND ALL "print("analyze" then command + / to toggle comments for debugging.

es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

# RETURNS COUNT OF EVENTS GROUPED BY TEAM.

def teams_with_data(index, doc_type):
	team_event_count = {}
	body = q_teams_with_data()
	response = es.search(index = index, doc_type = doc_type, size = 10000,  body = body)
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
	response = es.search(index = index, doc_type = doc_type, size = 10000, body = body)
	response_buckets = response.get("aggregations").get("repos").get("buckets")
	for element in response_buckets:
		team_active_repos.active_repos[element.get("key")] = {"events": element.get("doc_count")}
	return team_active_repos


# RETURNS FREQUENCY OF ANY COMBINATION OF TERM-ABLE QUERY, PASSED IN PARAMS.

def get_frequency(index, doc_type, params):
	average = Dict()
	key = index + doc_type
	key_append = []
	for attr,value in params.items():
		s = "&" + attr + "=" + value
		key_append.append(s)
	key_append.sort()
	key += "".join(key_append)
	# print("analyze.py line 51 get_frequency ------ key: {}".format(key))
	last_body = q_doc_by_id([key])
	last_body["sort"]["timestamp"]["order"] = "desc"
	last_average = es.search(index = "avglogs", doc_type = "avg", size = 1, body = last_body).get("hits")
	# print("analyze.py line 57 get_frequency ------ last_average: {}".format(last_average))

	if last_average.get("total") != 0:
		avg = update_frequency(index, doc_type, params, last_average)
		# print("analyze.py line 64 get_frequency > update_frequency ------ avg: {}".format(avg))
		average["key"] = key
		average["average"] = avg["average"]
		average["count"] = avg["count"]
		average["timestamp"] = datetime.datetime.now()
		# print("analyze.py line 67 get_frequency > update_frequency ------ average: {}".format(average))

	else:
		avg = new_frequency(index, doc_type, params)
		# print("analyze.py line 74 get_frequency > new_frequency ------ avg: {}".format(avg))
		average["key"] = key
		average["average"] = avg.get("average")
		average["count"] = avg.get("count")
		average["timestamp"] = datetime.datetime.now()
		# print("analyze.py line 76 get_frequency > new_frequency ------ average: {}".format(average))

	response = es.index(index='avglogs', doc_type='avg', id=key, body=average)
	# print("analyze.py line 82 get_frequency ------ insert into average index: {}".format(response))
	return average


# GENERATES THE FREQUENCY OF ANY COMBINATION OF TERM-ABLE QUERY, PASSED IN PARAMS, SAMPLES ALL EVENTS.

def new_frequency(index, doc_type, params):
	body = Dict()
	avg = Dict()
	# print("analyze.py line 93 new_frequency ------ params: {}".format(params))
	body = q_general_query(params, ["time"])
	body.sort.time.order = "asc"
	# print("analyze.py line 98 new_frequency ------ body: {}".format(body))
	events = es.search(index, doc_type, size = 10000, body = body).get("hits")
	# print("analyze.py line 100 new_frequency ------ events: {}".format(events))
	sum_diffs = sum(calculate_time_sum_diffs(events.get("hits")))
	# print("analyze.py line 100 new_frequency ------ sum_diffs: {}".format(sum_diffs))
	avg.count = events.get("total")
	# print("analyze.py line 102 new_frequency ------ count: {}".format(avg.count))
	avg.average = (sum_diffs / avg["count"])
	# print("analyze.py line 104 new_frequency ------ average: {}".format(avg.average))
	# print("analyze.py line 106 new_frequency ------ avg: {}".format(avg))
	return avg


# GENERATES THE FREQUENCY OF ANY COMBINATION OF TERM-ABLE QUERY, PASSED IN PARAMS, SAMPLES ONLY NEW EVENTS.

def update_frequency(index, doc_type, params, last_average):
	avg = Dict()
	new_body = Dict()
	date_min = last_average.get("hits")[0].get("_source").get("timestamp")
	date_max = datetime.datetime.now().isoformat('T')
	# print("analyze.py line 57 get_frequency > update_frequency ------ date_min: {}".format(date_min))
	# print("analyze.py line 58 get_frequency > update_frequency ------ date_max: {}".format(date_max))
	new_body = q_events_date_range(params, date_min, date_max, ["time"])
	# print("analyze.py line 61 get_frequency > update_frequency ------ new_body: {}".format(new_body))
	new_events = es.search(index = "static", doc_type = "index", size = 10000, body = new_body).get("hits")
	
	count_new = new_events.get("total")
	sum_diffs_new = sum(calculate_time_sum_diffs(new_events.get("hits")))
	average_old = last_average.get("hits")[0].get("_source").get("average")
	count_old = last_average.get("hits")[0].get("_source").get("count")
	avg.count = count_new + count_old
	avg.average = (((average_old * count_old) + sum_diffs_new) / (avg["count"]))
	# print("analyze.py line 118 get_frequency > update_frequency ------ avg: {}".format(avg))
	return avg


# CALCULATES THE TIME DIFFERENCE IN SECONDS BETWEEN EACH SEQUENTIAL EVENT PASSED TO IT IN AN ARRAY.

def calculate_time_sum_diffs(events):
	timestamps = []
	diffs_timedelta = []
	for event in events:
		# print("analyze.py line 131 calculate_time_sum_diffs ------ event: {}".format(event))
		time = event.get("_source").get("time")
		timestamps.append(dateparser.parse(time))
	# print("analyze.py line 111 calculate_time_sum_diffs ------ timestamps: {}".format(timestamps))
	sorted_timestamps = sorted(timestamps)
	# print("analyze.py line 117 calculate_time_sum_diffs ------ sorted timestamps: {}".format(sorted_timestamps))
	for i in range(len(sorted_timestamps)-1):
		diff = sorted_timestamps[i + 1] - sorted_timestamps[i]
		diffs_timedelta.append(diff.total_seconds())

	# print("analyze.py line 123 calculate_time_sum_diffs ------ calculated diffs: {}".format(diffs_timedelta))
	return diffs_timedelta


# GETS FREQUENCY OF ALL EVENTS.

def freq_events(index, doc_type):
	params = {}
	average = get_frequency(index, doc_type, params)
	return average


# GETS FREQUENCY OF EVENTS BY TYPE.

def freq_events_type(index, doc_type, event_type):
	params = {}
	params["event_type"] = event_type
	average = get_frequency(index, doc_type, params)
	return average


# GETS FREQUENCY OF ALL EVENTS BY TEAM.

def freq_events_team(index, doc_type, team):
	params = {}
	params["team"] = team
	average = get_frequency(index, doc_type, params)
	return average


# GETS FREQUENCY OF EVENTS BY TYPE AND TEAM.

def freq_events_type_team(index, doc_type, event_type, team):
	params = {}
	params["event_type"] = event_type
	params["team"] = team
	average = get_frequency(index, doc_type, params)
	return average


# GETS FREQUENCY OF EVENTS BY TEAM AND REPO.

def freq_events_team_repo(index, doc_type, team, repo):
	params = {}
	params["team"] = team
	params["repo"] = repo
	average = get_frequency(index, doc_type, params)
	return average


# GETS FREQUENCY OF EVENTS BY TYPE AND REPO.

def freq_events_type_repo(index, doc_type, event_type, repo):
	params = {}
	params["event_type"] = event_type
	params["repo"] = repo
	average = get_frequency(index, doc_type, params)
	return average


# GETS FREQUENCY OF EVENTS BY TYPE, TEAM, AND REPO.

def freq_events_type_team_repo(index, doc_type, event_type, team, repo):
	params = {}
	params["event_type"] = event_type
	params["team"] = team
	params["repo"] = repo
	average = get_frequency(index, doc_type, params)
	return average


# total number of teams with data [=== DONE ===]
print("Total number of teams with data:\n{}\n".format(teams_with_data("live", "events")))
# number of active repos in each team [=== DONE ===]
print("Number of active repos in each team:\n{}\n".format(active_repos_by_team("live", "events", "B")))
# total number of events captured overall [=== DONE ===]
print("Total number of events captured overall:\n{}\n".format(teams_with_data("live", "events")))
# total number of events captured per team [=== DONE ===]
# avg time between any captured events per team [=== DONE ===]
print("Total number of events captured per team & Avg time between any captured events per team:\n{}\n".format(freq_events_team("live", "events", "B")))
# frequency of event types captured overall [=== DONE ===]
print("Frequency of event types captured overall:\n{}\n".format(freq_events_type("live", "events", "push")))
# frequency of event types captured per team [=== DONE ===]
# avg time between same captured events per team [=== DONE ===]
print("Frequency of event types captured per team & Avg time between same captured events per team:\n{}\n".format(freq_events_type_team("live", "events", "push", "B")))
# frequency of event types captured per repo [=== DONE ===]
print("Frequency of event types captured per repo:\n{}\n".format(freq_events_type_team_repo("live", "events", "push", "B", "proj1")))
# avg time between any captured events per team [=== DONE ===]
print("Avg time between any captured events per team:\n{}\n".format(freq_events_type_team("live", "events", "push", "B")))






