select json_select('.taxon_name', jsondata) as xxx from testemunho where json_array_length(json_select('.taxon_name .autor:val("L.")', jsondata)) > 0 limit 300;

select count(*) from testemunho  where jsondata#>>'{historico_determinacoes,taxon_name,name_cache}' = 'Machaerium aculeatum';
select count(*) from testemunho  where jsondata->'coleta'->>'municipio' ilike 'Nova Igua%';

select count(*) from testemunho where jsondata->>'codigoBarra' ilike '%URB%';
select count(*) from testemunho where jsondata->>'codigoBarra' ilike 'FURB';

select * from testemunho t, json_array_elements(t.jsondata->'historico_determinacoes') hist
where hist#>>'{taxon_name,name_cache}' = 'Fabaceae'
limit 100;

select json2arr(jsonbdata, 'taxon_name') from testemunho limit 50

select count(*) from testemunho where (jsondata->'historico_determinacoes')::jsonb @> '{"codigoBarra": "FURB03672"}'::jsonb;

select count(*) from testemunho where (jsondata->'historico_determinacoes')::jsonb @> '{"coleta": {"municipio": "Nova Iguaçu"}}'::jsonb;

select count(*) from testemunho where (jsondata)::jsonb @> '{"historico_determinacoes": [{"taxon_name": {"name_cache": "Machaerium aculeatum"}}]}'::jsonb;
select count(*) from testemunho where (jsonbdata) @> '{"historico_determinacoes": [{"taxon_name": {"name_cache": "Machaerium aculeatum"}}]}';
ca


CREATE INDEX codigo_barra_index ON testemunho((jsondata->>'codigoBarra'));
CREATE INDEX estado_index ON testemunho((jsondata#>>'{coleta,estado}'));
CREATE INDEX data_inicial_ano_index ON testemunho((jsondata#>>'{coleta,data_inicial_ano}'));
CREATE INDEX default_taxon_genus_index ON testemunho((jsondata#>>'{determinacao_default,taxon_name,genus_or_uninomial}'));
CREATE INDEX default_taxon_cache_index ON testemunho((jsondata#>>'{determinacao_default,taxon_name,name_cache}'));

CREATE INDEX codigo_barra_index_b ON testemunho((jsonbdata->>'codigoBarra'));
CREATE INDEX estado_index_b ON testemunho((jsonbdata#>>'{coleta,estado}'));
CREATE INDEX data_inicial_ano_index_b ON testemunho((jsonbdata#>>'{coleta,data_inicial_ano}'));
CREATE INDEX default_taxon_genus_index_b ON testemunho((jsonbdata#>>'{determinacao_default,taxon_name,genus_or_uninomial}'));
CREATE INDEX default_taxon_cache_index_b ON testemunho((jsonbdata#>>'{determinacao_default,taxon_name,name_cache}'));

CREATE INDEX default_index_b ON testemunho USING gin ((jsonbdata -> 'determinacao_default'));
CREATE INDEX default_index_path_ops_b ON testemunho USING gin ((jsonbdata -> 'determinacao_default') jsonb_path_ops);
CREATE INDEX historico_index_b ON testemunho USING gin ((jsonbdata -> 'historico_determinacoes'));
CREATE INDEX historico_index_path_ops_b ON testemunho USING gin ((jsonbdata -> 'historico_determinacoes') jsonb_path_ops);

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










--CREATE INDEX ON testemunho USING gin ((jsonbdata -> 'historico_determinacoes' -> 'taxon_name') jsonb_path_ops);

alter table testemunho
	add column jsonbdata jsonb;

select * from testemunho limit 10

update testemunho set jsonbdata = jsondata::jsonb;

delete from testemunho;
