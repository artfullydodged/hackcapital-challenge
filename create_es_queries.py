from addict import Dict
import json


# GENERATES QUERY TO CREATE A NEW INDEX WITH MAPPING
# (INDEX MUST BE SPECIFIED IN URL)
# (CURRENTLY CUSTOMIZED FOR CREATING AVERAGES INDEX)

def create_index():
	body = Dict()
	body.mappings.avg.properties.key.type = "text"
	body.mappings.avg.properties.key.fields.keyword.type = "keyword"
	body.mappings.avg.properties.average.type = "integer"
	body.mappings.avg.properties.count.type = "integer"
	body.mappings.avg.properties.timestamp.type = "date"
	# pretty = json.dumps(body, indent=4, sort_keys=True)
	# return pretty
	return body


# GENERATES QUERY TO CREATE KEYWORD FIELD TO PROPERTIES IN MAPPING

def add_keyword_mapping():
	body = Dict()
	body.properties.repo.type = "text"
	body.properties.repo.fields.keyword.type = "keyword"
	# pretty = json.dumps(body, indent=4, sort_keys=True)
	# return pretty
	return body


# GENERATES QUERY TO GET ACTIVE REPOS BY TEAM

def q_active_repos_by_team(team, days):
	query = Dict()
	body = Dict()
	term_filter = Dict()
	date_range_filter = Dict()
	term = Dict() # < Not needed?
	multi_terms= []
	term_filter["term"]["team.keyword"] = team
	date_range_filter["range"].time.gte = "now-{}d/d".format(days) 
	date_range_filter["range"].time.lte = "now/d" 
	multi_terms.append(term_filter)
	multi_terms.append(date_range_filter)
	body.query.bool.must = multi_terms
	body.aggs.repos.terms.field = "repo.keyword"
	# pretty = json.dumps(body, indent=4, sort_keys=True)
	# return pretty
	return body


# GENERATES QUERY TO GET EVENTS BY DATE RANGE, OPTIONS IN PARAMS

def q_events_date_range(params, date_min, date_max, limit=None): #takes limit option as an array of fields to limit results to
	body = Dict()
	date_range_filter = Dict()
	multi_terms= []
	for attr,value in params.items():
		if value != "":
			new_term = Dict()
			new_term.term["{}.keyword".format(attr.replace("_", "."))] = value
			multi_terms.append(new_term)
	
	date_range_filter["range"].time.gte = "{}".format(date_min) 
	date_range_filter["range"].time.lte = "{}".format(date_max) 
	multi_terms.append(date_range_filter)
	body.query.bool.must = multi_terms
	if limit != None:
		body._source = limit
	# pretty = json.dumps(body, indent=4, sort_keys=True)
	# return pretty
	return body


# GENERATES QUERY TO GET EVENT COUNT FOR EVERY TEAM

def q_teams_with_data():
	body = Dict()
	body.size = 0
	body.aggs.teams.terms.field = "team.keyword"
	# pretty = json.dumps(body, indent=4, sort_keys=True)
	# return pretty
	return body


# GENERATES QUERY TO GET EVENT (OR AVERAGE) BY ID

def q_doc_by_id(ids_array): # Takes input ids as an array
	body = Dict()
	body.query.ids["values"] = ids_array
	# pretty = json.dumps(body, indent=4, sort_keys=True)
	# return pretty
	return body


# GENERATES QUERY TO GET EVENTS BY ANY TERM-ABLE OPTIONS, PASSED IN PARAMS

def	q_general_query(params, limit=None): #takes limit option as an array of fields to limit results to
	body = Dict()
	multi_terms= []
	for attr,value in params.items():
		if value != "":
			new_term = Dict()
			new_term.term["{}.keyword".format(attr.replace("_", "."))] = value
			multi_terms.append(new_term)

	body.query.bool.must = multi_terms
	if limit != None:
		body._source = limit
	# pretty = json.dumps(body, indent=4, sort_keys=True)
	# return pretty
	return body


