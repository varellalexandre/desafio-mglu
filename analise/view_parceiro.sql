WITH table_pedido_parceiro AS (
  SELECT fact_item_pedido.id_pedido
    , fact_item_pedido.dt_pedido
    , SUBSTR(fact_item_pedido.dt_pedido,1,7) as ano_mes
    , fact_item_pedido.quantidade
    , fact_item_pedido.vr_total_pago
    , dim_parceiro.id_parceiro
    , dim_parceiro.nm_parceiro
    , dim_produto.ds_produto
    , dim_produto.ds_subcategoria
    , dim_produto.ds_categoria
    , dim_produto.perc_parceiro
    , (dim_produto.perc_parceiro*fact_item_pedido.vr_total_pago)/100 AS comissao
  FROM fact_item_pedido
  LEFT JOIN dim_produto
  ON dim_produto.id_produto = fact_item_pedido.id_produto
  LEFT JOIN dim_parceiro
  ON dim_parceiro.id_parceiro = fact_item_pedido.id_parceiro
)

SELECT id_parceiro
    , nm_parceiro
    , ano_mes
    , ROUND(SUM(vr_total_pago),2) AS valor_total_vendido
    , ROUND(SUM(comissao),2) AS valor_total_de_comissao
    , FLOOR(SUM(comissao)/10000)*100 AS bonus
FROM table_pedido_parceiro
GROUP BY id_parceiro, ano_mes, nm_parceiro
