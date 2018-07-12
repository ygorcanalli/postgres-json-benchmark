-- 1) Testemunhos cuja determinação default é 'Machaerium aculeatum'

-- a)
SELECT count(*) FROM testemunho
WHERE jsonbdata #>> '{determinacao_default,taxon_name,name_cache}' = 'Machaerium aculeatum'

-- b)
SELECT count(*) FROM testemunho
WHERE jsonbdata @> '{"determinacao_default" : {"taxon_name": {"name_cache" : "Machaerium aculeatum"}}}'

-- 2) Testemunhos que já tiveram uma determinação 'Machaerium aculeatum'


SELECT count(*) FROM testemunho
WHERE jsonbdata @> '{"historico_determinacoes" : [{"taxon_name": {"name_cache" : "Machaerium aculeatum"}}]}'

-- 3) Testemunhos cuja determinação default é do gênero 'Swatzia' e foram coletadas no estado de 'Minas Gerais'

-- a)
select count(*) from testemunho
where jsonbdata #>> '{determinacao_default,taxon_name,genus_or_uninomial}' = 'Swartzia'
and jsonbdata #>> '{coleta,estado}' = 'Minas Gerais';

-- b)
SELECT count(*) FROM testemunho
WHERE jsonbdata @> '{"determinacao_default" : {"taxon_name": {"genus_or_uninomial" : "Swartzia"}}, "coleta" : {"estado": "Minas Gerais"}}'

-- 4) Testemunhos com coleta realizado entre 2000 e 2015 no rio de janeiro

SELECT count(*) FROM testemunho WHERE jsonbdata#>>'{coleta,data_inicial_ano}'  >= '2000' 
and jsonbdata#>>'{coleta,data_inicial_ano}' <= '2015'
and jsonbdata#>>'{coleta,estado}' = 'Rio de Janeiro'

-- 5) Testemunhos com codigo Barra contem B

select count(*) from testemunho where jsonbdata->>'codigoBarra' like '%B%';

-- 6) Testemunhos com codigo Barra igual RB00792761

-- a)
select count(*) from testemunho where jsonbdata->>'codigoBarra' = 'RB00792761';

-- b)
select count(*) from testemunho 
where jsonbdata @> '{"codigoBarra" : "RB00792761"}'

-- 7) Testemunhos que já tiveram uma determinação com gênero iniciando por 'Macha'

-- Sem solução




