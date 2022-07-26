# Nodrop_NOVO

As maquinas da CribMaster Geração 3 quando um item
retirado não cai o item precisa ser cancelado manualmente,
o sistema apenas gera um aviso indicando qual transação precisa ser cancelada.

O sistema localiza essa transação no banco de dados e efetua o cancelamento da mesma,
para que isso fosse realizado de modo seguro utilizamos um metodo de monitoramento de tudo que era
alterado no banco de dados quando um cancelamento manual era realizado.
