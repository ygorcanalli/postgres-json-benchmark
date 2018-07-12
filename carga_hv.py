# simple.py - very simple example of plain DBAPI-2.0 usage
#
# currently used as test-me-stress-me script for psycopg 2.0
#
# Copyright (C) 2001-2010 Federico Di Gregorio  <fog@debian.org>
#
# psycopg2 is free software: you can redistribute it and/or modify it
# under the terms of the GNU Lesser General Public License as published
# by the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# psycopg2 is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public
# License for more details.

## put in DSN your DSN string

DSN = "dbname='reflora_prod-20-08-2015' user='jabot' host='jabot.cos.ufrj.br' password='jabot'"
LOCAL_DSN = "dbname='benchmark_mp' user='mp' host='localhost' password='mp'"

## don't modify anything below this line (except for experimenting)

class SimpleQuoter(object):
    def sqlquote(x=None):
        return "'bar'"

import sys
import psycopg2
from psycopg2.extras import Json
import pprint
import json
from joblib import Parallel, delayed
from pymongo import MongoClient
import time

QUERY_INSERT_TESTEMUNHO_PG = "INSERT INTO testemunho (jsondata) values (%s)"

TESTEMUNHOS_PATH = "testemunhos.json"
QUERY_TESTEMUNHOS = """
SELECT
  testemunho.id, testemunho.codigo_barra, testemunho.numero_tombo,
  testemunho.status, testemunho.visualizacoes, testemunho.citacao_bibliografia,
  testemunho.numero_coleta_colecao_origem, testemunho.quantidade_duplicatas,
  testemunho.quantidade_duplicatas_estoque, testemunho.observacoes,
  coleta.numero, coleta.data_inicial_dia, coleta.data_inicial_mes, coleta.data_inicial_ano,
  coleta.data_final_dia, coleta.data_final_mes, coleta.data_final_ano,
  coleta.nome_expedicao, coleta.numero_coleta_expedicao,
  localidade.descricao_ambiente, localidade.descricao_localidade, localidade.elevacao_minima,
  localidade.elevacao_maxima, localidade.unidade_geopolitica, localidade.unidade_medida,
  localidade.ecossistema_tipo_vegetacao, localidade.codigo_area, localidade.pais,
  localidade.estado, localidade.municipio, localidade.coordenada_inferida,
  calcula_coordenada(lat_max.segundo, lat_max.minuto, lat_max.grau, lat_max.direcao_cardeal) lat_max,
  calcula_coordenada(lat_min.segundo, lat_min.minuto, lat_min.grau, lat_min.direcao_cardeal) lat_min,
  calcula_coordenada(lon_max.segundo, lon_max.minuto, lon_max.grau, lon_max.direcao_cardeal) lon_max,
  calcula_coordenada(lon_min.segundo, lon_min.minuto, lon_min.grau, lon_min.direcao_cardeal) lon_min,
  testemunho.determinacao_default_fk 
FROM testemunho
LEFT JOIN coleta on testemunho.coleta_fk = coleta.id
LEFT JOIN localidade ON coleta.localidade_fk = localidade.id
LEFT JOIN coordenada_geografica lat_max ON lat_max.id = localidade.latitude_maxima_fk
LEFT JOIN coordenada_geografica lat_min ON lat_min.id = localidade.latitude_minima_fk
LEFT JOIN coordenada_geografica lon_max ON lon_max.id = localidade.longitude_maxima_fk
LEFT JOIN coordenada_geografica lon_min ON lon_min.id = localidade.longitude_minima_fk"""

QUERY_DETERMINACAO = """
SELECT
  determinacao.*,
  tnb.rank,
  nvn.infra_generic_epithet, nvn.specific_epithet,
  nvn.subsp_specific_epithet, nvn.genus_or_uninomial,
  nvn.name_cache, nvn.autor, nvn.name_autor_cache,
  nvn.var_specific_epithet, nvn.form_specific_epithet, nvn.infra_specific_typical,
  tnb_familia.rank,
  nvn_familia.infra_generic_epithet, nvn_familia.specific_epithet,
  nvn_familia.subsp_specific_epithet, nvn_familia.genus_or_uninomial,
  nvn_familia.name_cache, nvn_familia.autor, nvn_familia.name_autor_cache,
  nvn_familia.var_specific_epithet, nvn_familia.form_specific_epithet, nvn_familia.infra_specific_typical
FROM determinacao
JOIN taxon_name_base tnb ON determinacao.taxon_name_base_fk = tnb.id
JOIN non_viral_name nvn ON tnb.id = nvn.id
JOIN taxon_name_base tnb_familia ON determinacao.familia_fk = tnb_familia.id
JOIN non_viral_name nvn_familia ON tnb_familia.id = nvn_familia.id
WHERE testemunho_fk = %d"""

QUERY_DETERMINACAO_DEFAULT = """
SELECT
  determinacao.*,
  tnb.rank,
  nvn.infra_generic_epithet, nvn.specific_epithet,
  nvn.subsp_specific_epithet, nvn.genus_or_uninomial,
  nvn.name_cache, nvn.autor, nvn.name_autor_cache,
  nvn.var_specific_epithet, nvn.form_specific_epithet, nvn.infra_specific_typical,
  tnb_familia.rank,
  nvn_familia.infra_generic_epithet, nvn_familia.specific_epithet,
  nvn_familia.subsp_specific_epithet, nvn_familia.genus_or_uninomial,
  nvn_familia.name_cache, nvn_familia.autor, nvn_familia.name_autor_cache,
  nvn_familia.var_specific_epithet, nvn_familia.form_specific_epithet, nvn_familia.infra_specific_typical
FROM determinacao
JOIN taxon_name_base tnb ON determinacao.taxon_name_base_fk = tnb.id
JOIN non_viral_name nvn ON tnb.id = nvn.id
JOIN taxon_name_base tnb_familia ON determinacao.familia_fk = tnb_familia.id
JOIN non_viral_name nvn_familia ON tnb_familia.id = nvn_familia.id
WHERE determinacao.id = %d"""

if len(sys.argv) > 1:
    DSN = sys.argv[1]


#postgres connections
print("Opening connection using dsn:", DSN)
conn = psycopg2.connect(DSN)
print("Encoding for this connection is", conn.encoding)


#mongo dg connection
mongo_conn = MongoClient('localhost', 27017, j=True)
#mongo_conn.write_concern = {'w':1, 'j':True}
mongo_db = mongo_conn['benchmark_mp']


def monta_determinacao(determinacao_row):
    determinacao = dict()
    if determinacao_row[0] is not None:
        determinacao['id'] = determinacao_row[0]
    if determinacao_row[2] is not None:
        determinacao['typus'] = determinacao_row[2]
    if determinacao_row[3] is not None:
        determinacao['natureza_typus'] = determinacao_row[3]
    if determinacao_row[4] is not None:
        determinacao['identificacao'] = determinacao_row[4]
    if determinacao_row[5] is not None:
        determinacao['determinador'] = determinacao_row[5]
    if determinacao_row[6] is not None:
        determinacao['data_dia'] = determinacao_row[6]
    if determinacao_row[7] is not None:
        determinacao['data_mes'] = determinacao_row[7]
    if determinacao_row[8] is not None:
        determinacao['data_ano'] = determinacao_row[8]
    if determinacao_row[9] is not None:
        determinacao['confirmacao'] = determinacao_row[9]
    if determinacao_row[10] is not None:
        determinacao['determinador_original'] = determinacao_row[10]
    if determinacao_row[11] is not None:
        determinacao['data_determinacao_original'] = determinacao_row[11].isoformat()
    if determinacao_row[12] is not None:
        determinacao['observacoes'] = determinacao_row[12]
    if determinacao_row[13] is not None:
        determinacao['data_dia_original'] = determinacao_row[13]
    if determinacao_row[14] is not None:
        determinacao['data_mes_original'] = determinacao_row[14]
    if determinacao_row[15] is not None:
        determinacao['data_ano_original'] = determinacao_row[15]
    if determinacao_row[20] is not None:
        determinacao['data_criacao_no_sistema'] = determinacao_row[20].isoformat()
    if determinacao_row[21] is not None:
        determinacao['data'] = determinacao_row[21].isoformat()
    if determinacao_row[22] is not None:
        determinacao['id_import'] = determinacao_row[22]
    if determinacao_row[23] is not None:
        determinacao['eh_default'] = determinacao_row[23]
    if determinacao_row[24] is not None:
        determinacao['associada_voucher'] = determinacao_row[24]
    if determinacao_row[25] is not None:
        determinacao['is_carga'] = determinacao_row[25]

    taxon_name = dict()
    if determinacao_row[26] is not None:
        taxon_name['rank'] = determinacao_row[26]
    if determinacao_row[27] is not None:
        taxon_name['infra_generic_epithet'] = determinacao_row[27]
    if determinacao_row[28] is not None:
        taxon_name['specific_epithet'] = determinacao_row[28]
    if determinacao_row[29] is not None:
        taxon_name['subsp_specific_epithet'] = determinacao_row[29]
    if determinacao_row[30] is not None:
        taxon_name['genus_or_uninomial'] = determinacao_row[30]
    if determinacao_row[31] is not None:
        taxon_name['name_cache'] = determinacao_row[31]
    if determinacao_row[32] is not None:
        taxon_name['autor'] = determinacao_row[32]
    if determinacao_row[33] is not None:
        taxon_name['name_autor_cache'] = determinacao_row[33]
    if determinacao_row[34] is not None:
        taxon_name['var_specific_epithet'] = determinacao_row[34]
    if determinacao_row[35] is not None:
        taxon_name['form_specific_epithet'] = determinacao_row[35]
    if determinacao_row[36] is not None:
        taxon_name['infra_specific_typical'] = determinacao_row[36]

    determinacao['taxon_name'] = taxon_name

    family_name = dict()
    if determinacao_row[37] is not None:
        family_name['rank'] = determinacao_row[37]
    if determinacao_row[38] is not None:
        family_name['infra_generic_epithet'] = determinacao_row[38]
    if determinacao_row[39] is not None:
        family_name['specific_epithet'] = determinacao_row[39]
    if determinacao_row[40] is not None:
        family_name['subsp_specific_epithet'] = determinacao_row[40]
    if determinacao_row[41] is not None:
        family_name['genus_or_uninomial'] = determinacao_row[41]
    if determinacao_row[42] is not None:
        family_name['name_cache'] = determinacao_row[42]
    if determinacao_row[43] is not None:
        family_name['autor'] = determinacao_row[43]
    if determinacao_row[44] is not None:
        family_name['name_autor_cache'] = determinacao_row[44]
    if determinacao_row[45] is not None:
        family_name['var_specific_epithet'] = determinacao_row[45]
    if determinacao_row[46] is not None:
        family_name['form_specific_epithet'] = determinacao_row[46]
    if determinacao_row[47] is not None:
        family_name['infra_specific_typical'] = determinacao_row[47]

    determinacao['family_name'] = family_name

    return determinacao

def monta_testemunho(testemunho_row):
    testemunhoJson = dict()
    if testemunho_row[0] is not None:
        testemunhoJson['id'] = testemunho_row[0]
    if testemunho_row[1] is not None:
        testemunhoJson['codigoBarra'] = testemunho_row[1]
    if testemunho_row[2] is not None:
        testemunhoJson['tombo'] = testemunho_row[2]
    if testemunho_row[3] is not None:
        testemunhoJson['status'] = testemunho_row[3]
    if testemunho_row[4] is not None:
        testemunhoJson['vizualizacoes'] = testemunho_row[4]
    if testemunho_row[5] is not None:
        testemunhoJson['citacao'] = testemunho_row[5]
    if testemunho_row[6] is not None:
        testemunhoJson['numeroColetaColecaoOrigem'] = testemunho_row[6]
    if testemunho_row[7] is not None:
        testemunhoJson['quantidadeDuplicadas'] = testemunho_row[7]
    if testemunho_row[8] is not None:
        testemunhoJson['quantidadeDuplicadasEstoque'] = testemunho_row[8]
    if testemunho_row[9] is not None:
        testemunhoJson['observacoes'] = testemunho_row[9]

    coleta = dict()
    if testemunho_row[10] is not None:
        coleta['numero'] = testemunho_row[10]
    if testemunho_row[11] is not None:
        coleta['data_inicial_dia'] = testemunho_row[11]
    if testemunho_row[12] is not None:
        coleta['data_inicial_mes'] = testemunho_row[12]
    if testemunho_row[13] is not None:
        coleta['data_inicial_ano'] = testemunho_row[13]
    if testemunho_row[14] is not None:
        coleta['data_final_dia'] = testemunho_row[14]
    if testemunho_row[15] is not None:
        coleta['data_final_mes'] = testemunho_row[15]
    if testemunho_row[16] is not None:
        coleta['data_final_ano'] = testemunho_row[16]
    if testemunho_row[17] is not None:
        coleta['nome_expedicao'] = testemunho_row[17]
    if testemunho_row[18] is not None:
        coleta['numero_coleta_expedicao'] = testemunho_row[18]
    if testemunho_row[19] is not None:
        coleta['descricao_ambiente'] = testemunho_row[19]
    if testemunho_row[20] is not None:
        coleta['descricao_localidade'] = testemunho_row[20]
    if testemunho_row[21] is not None:
        coleta['elevacao_minima'] = testemunho_row[21]
    if testemunho_row[22] is not None:
        coleta['elevacao_maxima'] = testemunho_row[22]
    if testemunho_row[23] is not None:
        coleta['unidade_geopolitica'] = testemunho_row[23]
    if testemunho_row[24] is not None:
        coleta['unidade_medida'] = testemunho_row[24]
    if testemunho_row[25] is not None:
        coleta['ecossistema_tipo_vegetacao'] = testemunho_row[25]
    if testemunho_row[26] is not None:
        coleta['codigo_area'] = testemunho_row[26]
    if testemunho_row[27] is not None:
        coleta['pais'] = testemunho_row[27]
    if testemunho_row[28] is not None:
        coleta['estado'] = testemunho_row[28]
    if testemunho_row[29] is not None:
        coleta['municipio'] = testemunho_row[29]
    if testemunho_row[30] is not None:
        coleta['coordenada_inferida'] = testemunho_row[30]
    if testemunho_row[31] is not None:
        coleta['latitude_maxima'] = testemunho_row[31]
    if testemunho_row[32] is not None:
        coleta['latitude_minima'] = testemunho_row[32]
    if testemunho_row[33] is not None:
        coleta['longitude_maxima'] = testemunho_row[33]
    if testemunho_row[34] is not None:
        coleta['longitude_mminima'] = testemunho_row[34]


    testemunhoJson['coleta'] = coleta
        
    conn2 = psycopg2.connect(DSN)

    # determinacao default
    if testemunho_row[35] is not None:
        determinacao_default_fk = testemunho_row[35]
        curs2 = conn2.cursor()
        curs2.execute(QUERY_DETERMINACAO_DEFAULT % determinacao_default_fk)
        determinacao_default_row = curs2.fetchone()
        if determinacao_default_row is not None:
            testemunhoJson['determinacao_default'] = monta_determinacao(determinacao_default_row)
        curs2.close()

    # historico de determinacoes
    historico_determinacoes = []
    curs3 = conn2.cursor()
    curs3.execute(QUERY_DETERMINACAO % testemunhoJson['id'])
    for determinacao_row in curs3.fetchall():
        determinacao = monta_determinacao(determinacao_row)
        historico_determinacoes.append(determinacao)

    curs3.close()

    if len(historico_determinacoes) > 0:
        testemunhoJson['historico_determinacoes'] = historico_determinacoes

    #store on posgres
    local_conn = psycopg2.connect(LOCAL_DSN)
    local_curs = local_conn.cursor()
    local_curs.execute(QUERY_INSERT_TESTEMUNHO_PG % Json(testemunhoJson))
    local_curs.execute("COMMIT;")
    local_curs.close()
    #store on mongo
    mongo_testemunho = mongo_db['testemunho']
    mongo_testemunho.insert_one(testemunhoJson)

curs = conn.cursor()
curs.execute(QUERY_TESTEMUNHOS)

for testemunho_row in curs.fetchall():
    monta_testemunho(testemunho_row)

mongo_conn.close()     
