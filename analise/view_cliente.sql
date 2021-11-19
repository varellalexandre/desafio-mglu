WITH table_pedido_cliente AS (
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
    , dim_cliente.id_cliente
    , dim_cliente.nm_cliente
    , dim_cliente.flag_ouro
  FROM fact_item_pedido
  LEFT JOIN dim_produto
  ON dim_produto.id_produto = fact_item_pedido.id_produto
  LEFT JOIN dim_parceiro
  ON dim_parceiro.id_parceiro = fact_item_pedido.id_parceiro
  LEFT JOIN dim_cliente
  ON dim_cliente.id_cliente = fact_item_pedido.id_cliente
)

SELECT id_cliente
    , nm_cliente
    , ano_mes
    , flag_ouro
    , ROUND(SUM(vr_total_pago),2) AS valor_total_vendido
    , ROUND(SUM(comissao),2) AS valor_total_de_comissao
    , FLOOR(SUM(comissao)/10000)*100 AS bonus
FROM table_pedido_cliente
GROUP BY ano_mes, id_cliente, nm_cliente, flag_ouro
