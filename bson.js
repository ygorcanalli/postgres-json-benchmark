// 1) Testemunhos cuja determinação default é 'Machaerium aculeatum'

db.testemunho.count( {'determinacao_default.taxon_name.name_cache': 'Machaerium aculeatum'} );

// 2) Testemunhos que já tiveram uma determinação 'Machaerium aculeatum'

db.testemunho.count( {'historico_determinacoes.taxon_name.name_cache': 'Machaerium aculeatum'} );

// 3) Testemunhos cuja determinação default é do gênero 'Swatzia' e foram coletadas no estado de 'Minas Gerais'

db.testemunho.count( { $and : [{'determinacao_default.taxon_name.genus_or_uninomial' : 'Swartzia'}, {'coleta.estado' : 'Minas Gerais'}]} );

// 4) Testemunhos com coleta realizado entre 2000 e 2015 no rio de janeiro
 db.testemunho.count( { $and: [ {'coleta.data_inicial_ano' : { $gte : 2000, $lte : 2015}}, {'coleta.estado': "Rio de Janeiro" }]} );

// 5) Testemunhos com codigo Barra contem B
db.testemunho.count({codigoBarra: {$regex: 'B'}});

// 6) Testemunhos com codigo Barra igual RB00792761
db.testemunho.count({codigoBarra:'RB00792761'});

// 7) Testemunhos que já tiveram uma determinação com gênero iniciando por 'Macha'

db.testemunho.count( {'historico_determinacoes.taxon_name.genus_or_uninomial': { $regex : '^Macha'}} );

//INDEXES

db.testemunho.createIndex( { 'determinacao_default.taxon_name.name_cache': 1 } );
db.testemunho.createIndex( { 'historico_determinacoes.taxon_name.name_cache': 1 } );
db.testemunho.createIndex( { 'determinacao_default.taxon_name.genus_or_uninomial': 1 } );
db.testemunho.createIndex( { 'historico_determinacoes.taxon_name.genus_or_uninomial': 1 } );
db.testemunho.createIndex( { 'coleta.estado': 1 } );
db.testemunho.createIndex( { 'coleta.data_inicial_ano': 1 } );
db.testemunho.createIndex( { 'codigoBarra': 1 } );

db.testemunho.dropIndexes();