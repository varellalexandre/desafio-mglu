SELECT item_pedido.id_pedido
  , pedido.dt_pedido
  , pedido.id_parceiro
  , pedido.id_cliente
  , pedido.id_filial
  , item_pedido.id_produto
  , item_pedido.quantidade
  , item_pedido.vr_unitario
  , (item_pedido.vr_unitario * item_pedido.quantidade) AS vr_total_pago
FROM item_pedido
INNER JOIN pedido
ON pedido.id_pedido = item_pedido.id_pedido
WHERE pedido.dt_pedido >= '{date_from}'
ORDER BY pedido.dt_pedido ASC
