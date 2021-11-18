SELECT filial.id_filial
  , filial.ds_filial
  , cidade.ds_cidade
  , estado.ds_estado
FROM filial
LEFT JOIN cidade
ON cidade.id_cidade = filial.id_cidade
LEFT JOIN estado
ON estado.id_estado = cidade.id_estado
