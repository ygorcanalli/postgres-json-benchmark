import json
import numpy as np
from pprint import pprint

with open('output/mongo_times.json') as data_file:    
    data = json.load(data_file)
    print("Mongo DB without index")
    for key, value in data.items():
    	mean = np.mean(value)
    	print("Query C%s: %F" % (key, mean))

with open('output/jsonb_times.json') as data_file:    
    data = json.load(data_file)
    print("\nPostgres JSONB without index")
    for key, value in data.items():
    	mean = np.mean(value)
    	print("Query C%s: %F" % (key, mean))

with open('output/json_times.json') as data_file:    
    data = json.load(data_file)
    print("\nPostgres JSON without index")
    for key, value in data.items():
    	mean = np.mean(value)
    	print("Query C%s: %F" % (key, mean))

with open('output/idx_mongo_times.json') as data_file:    
    data = json.load(data_file)
    print("\nMongo DB with index")
    for key, value in data.items():
    	mean = np.mean(value)
    	print("Query C%s: %F" % (key, mean))

with open('output/idx_jsonb_times.json') as data_file:    
    data = json.load(data_file)
    print("\nPostgres JSONB with index")
    for key, value in data.items():
    	mean = np.mean(value)
    	print("Query C%s: %F" % (key, mean))

with open('output/idx_json_times.json') as data_file:    
    data = json.load(data_file)
    print("\nPostgres JSON with index")
    for key, value in data.items():
    	mean = np.mean(value)
    	print("Query C%s: %F" % (key, mean))
