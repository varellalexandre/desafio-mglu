SELECT produto.id_produto
  , produto.ds_produto
  , subcategoria.ds_subcategoria
  , categoria.ds_categoria
  , categoria.perc_parceiro
FROM produto
LEFT JOIN subcategoria
ON subcategoria.id_subcategoria = produto.id_subcategoria
LEFT JOIN categoria
ON categoria.id_categoria = subcategoria.id_categoria
