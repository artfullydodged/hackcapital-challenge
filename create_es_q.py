from addict import Dict
import json

def add_keyword_mapping():
	body = Dict()
	body.properties.repo.type = "text"
	body.properties.repo.fields.keyword.type = "keyword"
	return json.dumps(body, indent=4, sort_keys=True)

def active_repos_by_team(team, days):
	query = Dict()
	body = Dict()
	term_filter = Dict()
	date_range_filter = Dict()
	term = Dict()
	multi_terms= []
	term_filter["term"]["team.keyword"] = team
	date_range_filter["range"].time.gte = "now-{}d/d".format(days) 
	date_range_filter["range"].time.lte = "now/d" 
	multi_terms.append(term_filter)
	multi_terms.append(date_range_filter)
	body.query.bool.must = multi_terms
	body.aggs.repos.terms.field = "repo.keyword"
	return json.dumps(body, indent=4, sort_keys=True)

def teams_with_data():
	body = Dict()
	body.size = 0
	body.aggs.teams.terms.field = "team.keyword"
	return json.dumps(body, indent=4, sort_keys=True)

# print(teams_with_data())
print(active_repos_by_team())


# aggs.repos.filter.term.team = 
# query.bool.must = []
# term.team = "B"
# term.
# query.bool.must = []
# term_filter = Dict()
# date_range_filter = Dict()
# term_filter["term"].team = "B"
# date_range_filter["range"].date.gte = "now-14d/d" 
# date_range_filter["range"].date.lte = "now/d" 

# body.query.bool.must.append(term_filter)
# body.query.bool.must.append(term.date_range_filter)
# body.aggs.repos.aggs.repo_activity.terms.field.repo

# "aggs" : {
#         "t_shirts" : {
#             "filter" : { "term": { "type": "t-shirt" } },
#             "aggs" : {
#                 "avg_price" : { "avg" : { "field" : "price" } }
#             }
#         }
#     }

#         "range": {
#             "date_range": {
#                 "field": "date",
#                 "format": "MM-yyy",
#                 "ranges": [
#                     { "to": "now-10M/M" }, 
#                     { "from": "now-10M/M" } 
#                 ]
#             }
#         }

# {
#   "query": {
#     "bool": {
#       "must":     { "match": { "title": "quick" }},
#       "must_not": { "match": { "title": "lazy"  }},
#       "should": [
#                   { "match": { "title": "brown" }},
#                   { "match": { "title": "dog"   }}
#       ]
#     }
#   }
# }




