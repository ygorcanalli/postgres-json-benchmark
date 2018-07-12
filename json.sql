-- 1) Testemunhos cuja determinação default é 'Machaerium aculeatum'

SELECT count(*) FROM testemunho
WHERE jsondata #>> '{determinacao_default,taxon_name,name_cache}' = 'Machaerium aculeatum';

-- 2) Testemunhos que já tiveram uma determinação 'Machaerium aculeatum'

-- Sem solução

-- 3) Testemunhos cuja determinação default é do gênero 'Swatzia' e foram coletadas no estado de 'Minas Gerais'

select count(*) from testemunho
where jsondata #>> '{determinacao_default,taxon_name,genus_or_uninomial}' = 'Swartzia'
and jsondata #>> '{coleta,estado}' = 'Minas Gerais';

-- 4) Testemunhos com coleta realizado entre 2000 e 2015 no rio de janeiro
SELECT count(*) FROM testemunho WHERE jsondata#>>'{coleta,data_inicial_ano}'  >= '2000' 
and jsondata#>>'{coleta,data_inicial_ano}' <= '2015'
and jsondata#>>'{coleta,estado}' = 'Rio de Janeiro';

-- 5) Testemunhos com codigo Barra contem B
select count(*) from testemunho where jsondata->>'codigoBarra' like '%B%';

-- 6) Testemunhos com codigo Barra igual RB00792761
select count(*) from testemunho where jsondata->>'codigoBarra' = 'RB00792761';

-- 7) Testemunhos que já tiveram uma determinação com gênero iniciando por 'Macha'

-- Sem solução


