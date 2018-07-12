import sys
import psycopg2
from psycopg2.extras import Json
from pprint import pprint
import json
from joblib import Parallel, delayed
from pymongo import MongoClient
import time

DSN = "dbname='benchmark_mp' user='mp' host='localhost' password='mp'"

n = 30

json_path = 'output/json_times.json'
jsonb_path = 'output/jsonb_times.json'
mongo_path = 'output/mongo_times.json'

pg_json_times = dict()
pg_jsonb_times = dict()
mongo_times = dict()

idx_json_path = 'output/idx_json_times.json'
idx_jsonb_path = 'output/idx_jsonb_times.json'
idx_mongo_path = 'output/idx_mongo_times.json'

idx_pg_json_times = dict()
idx_pg_jsonb_times = dict()
idx_mongo_times = dict()


pg_json_queries = dict()
pg_jsonb_queries = dict()
mongo_queries = dict()

pg_json_queries['1'] = 'SELECT count(*) FROM testemunho WHERE jsondata #>> \'{determinacao_default,taxon_name,name_cache}\' = \'Machaerium aculeatum\';'
pg_json_queries['3'] = 'SELECT count(*) FROM testemunho WHERE jsondata #>> \'{determinacao_default,taxon_name,genus_or_uninomial}\' = \'Swartzia\' and jsondata #>> \'{coleta,estado}\' = \'Minas Gerais\';'
pg_json_queries['4'] = 'SELECT count(*) FROM testemunho WHERE jsondata #>> \'{coleta,data_inicial_ano}\'  >= \'2000\' and jsondata #>> \'{coleta,data_inicial_ano}\' <= \'2015\' and jsondata #>> \'{coleta,estado}\' = \'Rio de Janeiro\';'
pg_json_queries['5'] = 'SELECT count(*) FROM testemunho WHERE jsondata ->> \'codigoBarra\' like \'%B%\';'
pg_json_queries['6'] = 'SELECT count(*) FROM testemunho WHERE jsondata ->> \'codigoBarra\' = \'RB00792761\';'

pg_jsonb_queries['1a'] = 'SELECT count(*) FROM testemunho WHERE jsonbdata #>> \'{determinacao_default,taxon_name,name_cache}\' = \'Machaerium aculeatum\';'
pg_jsonb_queries['1b'] = 'SELECT count(*) FROM testemunho WHERE jsonbdata @> \'{"determinacao_default" : {"taxon_name": {"name_cache" : "Machaerium aculeatum"}}}\';'
pg_jsonb_queries['2']  = 'SELECT count(*) FROM testemunho WHERE jsonbdata @> \'{"historico_determinacoes" : [{"taxon_name": {"name_cache" : "Machaerium aculeatum"}}]}\';'
pg_jsonb_queries['3a'] = 'SELECT count(*) FROM testemunho WHERE jsonbdata #>> \'{determinacao_default,taxon_name,genus_or_uninomial}\' = \'Swartzia\' and jsonbdata #>> \'{coleta,estado}\' = \'Minas Gerais\';'
pg_jsonb_queries['3b'] = 'SELECT count(*) FROM testemunho WHERE jsonbdata @> \'{"determinacao_default" : {"taxon_name": {"genus_or_uninomial" : "Swartzia"}}, "coleta" : {"estado": "Minas Gerais"}}\';' 
pg_jsonb_queries['4']  = 'SELECT count(*) FROM testemunho WHERE jsonbdata #>> \'{coleta,data_inicial_ano}\' >= \'2000\' and jsonbdata #>> \'{coleta,data_inicial_ano}\' <= \'2015\' and jsonbdata #>> \'{coleta,estado}\' = \'Rio de Janeiro\';'
pg_jsonb_queries['5']  = 'SELECT count(*) FROM testemunho WHERE jsonbdata ->> \'codigoBarra\' like \'%B%\';'
pg_jsonb_queries['6a'] = 'SELECT count(*) FROM testemunho WHERE jsonbdata ->> \'codigoBarra\' = \'RB00792761\';'
pg_jsonb_queries['6b'] = 'SELECT count(*) FROM testemunho WHERE jsonbdata @> \'{"codigoBarra" : "RB00792761"}\';'

mongo_queries['1'] = {"determinacao_default.taxon_name.name_cache": "Machaerium aculeatum"}
mongo_queries['2'] = {"historico_determinacoes.taxon_name.name_cache": "Machaerium aculeatum"}
mongo_queries['3'] = { "$and" : [{"determinacao_default.taxon_name.genus_or_uninomial" : "Swartzia"}, {"coleta.estado" : "Minas Gerais"}]}
mongo_queries['4'] = { "$and": [ {"coleta.data_inicial_ano" : { "$gte" : 2000, "$lte" : 2015}}, {"coleta.estado": "Rio de Janeiro" }]}
mongo_queries['5'] = {"codigoBarra": {"$regex": "B"}}
mongo_queries['6'] = {"codigoBarra":"RB00792761"}
mongo_queries['7'] = {"historico_determinacoes.taxon_name.genus_or_uninomial": { "$regex" : "^Macha"}}

pg_drop_indexes = """
DROP INDEX IF EXISTS codigo_barra_index;
DROP INDEX IF EXISTS estado_index;
DROP INDEX IF EXISTS data_inicial_ano_index;
DROP INDEX IF EXISTS default_taxon_genus_index;
DROP INDEX IF EXISTS default_taxon_cache_index;

DROP INDEX IF EXISTS codigo_barra_index_b;
DROP INDEX IF EXISTS estado_index_b;
DROP INDEX IF EXISTS data_inicial_ano_index_b;
DROP INDEX IF EXISTS default_taxon_genus_index_b;
DROP INDEX IF EXISTS default_taxon_cache_index_b;

DROP INDEX IF EXISTS default_index_b;
DROP INDEX IF EXISTS default_index_path_ops_b;
DROP INDEX IF EXISTS historico_index_b;
DROP INDEX IF EXISTS historico_index_path_ops_b;
COMMIT;
"""

pg_create_indexes = """
CREATE INDEX codigo_barra_index ON testemunho((jsondata->>'codigoBarra'));
COMMIT;
CREATE INDEX estado_index ON testemunho((jsondata#>>'{coleta,estado}'));
COMMIT;
CREATE INDEX data_inicial_ano_index ON testemunho((jsondata#>>'{coleta,data_inicial_ano}'));
COMMIT;
CREATE INDEX default_taxon_genus_index ON testemunho((jsondata#>>'{determinacao_default,taxon_name,genus_or_uninomial}'));
COMMIT;
CREATE INDEX default_taxon_cache_index ON testemunho((jsondata#>>'{determinacao_default,taxon_name,name_cache}'));
COMMIT;

CREATE INDEX codigo_barra_index_b ON testemunho((jsonbdata->>'codigoBarra'));
COMMIT;
CREATE INDEX estado_index_b ON testemunho((jsonbdata#>>'{coleta,estado}'));
COMMIT;
CREATE INDEX data_inicial_ano_index_b ON testemunho((jsonbdata#>>'{coleta,data_inicial_ano}'));
COMMIT;
CREATE INDEX default_taxon_genus_index_b ON testemunho((jsonbdata#>>'{determinacao_default,taxon_name,genus_or_uninomial}'));
COMMIT;
CREATE INDEX default_taxon_cache_index_b ON testemunho((jsonbdata#>>'{determinacao_default,taxon_name,name_cache}'));
COMMIT;

CREATE INDEX default_index_b ON testemunho USING gin ((jsonbdata -> 'determinacao_default'));
COMMIT;
CREATE INDEX default_index_path_ops_b ON testemunho USING gin ((jsonbdata -> 'determinacao_default') jsonb_path_ops);
COMMIT;
CREATE INDEX historico_index_b ON testemunho USING gin ((jsonbdata -> 'historico_determinacoes'));
COMMIT;
CREATE INDEX historico_index_path_ops_b ON testemunho USING gin ((jsonbdata -> 'historico_determinacoes') jsonb_path_ops);
COMMIT;
"""

for k in pg_json_queries.keys():
	pg_json_times[k] = []
	idx_pg_json_times[k] = []

for k in pg_jsonb_queries.keys():
	pg_jsonb_times[k] = []
	idx_pg_jsonb_times[k] = []

for k in mongo_queries.keys():
	mongo_times[k] = []
	idx_mongo_times[k] = []

# =============== without indexes ===============

#postgres connections
print("Opening connection using dsn:", DSN)
pg_conn = psycopg2.connect(DSN)
print("Encoding for this connection is", pg_conn.encoding)

#drop all indexes
curs = pg_conn.cursor()
curs.execute(pg_drop_indexes)

print("Postgres JSON without index")
for i in range(n):
	print(i)
	for k, q in pg_json_queries.items():
		curs = pg_conn.cursor()
		begin = time.time()
		curs.execute(q)
		end = time.time()
		pg_json_times[k].append( end - begin)
		
		with open(json_path, "w") as f:		
			json.dump(pg_json_times, f, sort_keys=True, indent=4)

print("Postgres JSONB without index")
for i in range(n):
	print(i)
	for k, q in pg_jsonb_queries.items():
		curs = pg_conn.cursor()
		begin = time.time()
		curs.execute(q)
		end = time.time()
		pg_jsonb_times[k].append( end - begin)

		with open(jsonb_path, "w") as f:		
			json.dump(pg_jsonb_times, f, sort_keys=True, indent=4)


pg_conn.close()

#mongo db connection
mongo_conn = MongoClient('localhost', 27017, j=True)
#mongo_conn.write_concern = {'w':1, 'j':True}
mongo_db = mongo_conn['benchmark_mp']
mongo_testemunho = mongo_db['testemunho']

#drop all indexes
mongo_testemunho.drop_indexes()

print("Mongo without index")
for i in range(n):
	print(i)
	for k, q in mongo_queries.items():
		begin = time.time()
		mongo_testemunho.count( q )
		end = time.time()
		mongo_times[k].append( end - begin)

		with open(mongo_path, "w") as f:		
			json.dump(mongo_times, f, sort_keys=True, indent=4)

mongo_conn.close()

# =============== with indexes ===============

#postgres connections
print("Opening connection using dsn:", DSN)
pg_conn = psycopg2.connect(DSN)
print("Encoding for this connection is", pg_conn.encoding)

#create all indexes

print("Postgres creating index")
curs = pg_conn.cursor()
curs.execute(pg_create_indexes)

print("Postgres JSON with index")
for i in range(n):
	print(i)
	for k, q in pg_json_queries.items():
		curs = pg_conn.cursor()
		begin = time.time()
		curs.execute(q)
		end = time.time()
		idx_pg_json_times[k].append( end - begin)
		
		with open(idx_json_path, "w") as f:		
			json.dump(idx_pg_json_times, f, sort_keys=True, indent=4)

print("Postgres JSONB with index")
for i in range(n):
	print(i)
	for k, q in pg_jsonb_queries.items():
		curs = pg_conn.cursor()
		begin = time.time()
		curs.execute(q)
		end = time.time()
		idx_pg_jsonb_times[k].append( end - begin)

		with open(idx_jsonb_path, "w") as f:		
			json.dump(idx_pg_jsonb_times, f, sort_keys=True, indent=4)


pg_conn.close()

#mongo db connection
mongo_conn = MongoClient('localhost', 27017, j=True)
#mongo_conn.write_concern = {'w':1, 'j':True}
mongo_db = mongo_conn['benchmark_mp']
mongo_testemunho = mongo_db['testemunho']


print("Mongo creating index")
#create all indexes
mongo_testemunho.create_index( "determinacao_default.taxon_name.name_cache" );
mongo_testemunho.create_index( "historico_determinacoes.taxon_name.name_cache" );
mongo_testemunho.create_index( "determinacao_default.taxon_name.genus_or_uninomial" );
mongo_testemunho.create_index( "historico_determinacoes.taxon_name.genus_or_uninomial" );
mongo_testemunho.create_index( "coleta.estado" );
mongo_testemunho.create_index( "coleta.data_inicial_ano" );
mongo_testemunho.create_index( "codigoBarra" );

print("Mongo with index")
for i in range(n):
	print(i)
	for k, q in mongo_queries.items():
		begin = time.time()
		mongo_testemunho.count( q )
		end = time.time()
		idx_mongo_times[k].append( end - begin)

		with open(idx_mongo_path, "w") as f:		
			json.dump(idx_mongo_times, f, sort_keys=True, indent=4)

mongo_conn.close()
