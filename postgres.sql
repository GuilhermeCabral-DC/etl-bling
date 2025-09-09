Select * from
fato.pedido_venda_item pedido
LEFT JOIN dim.produto produto ON pedido.id_produto = produto.id_produto
Where formato = 'E'