-- Databricks notebook source
with tb_activate as (
select distinct idVendedor, min(date(dtPedido)) as dtAtivacao
from silver.olist.pedido as t1
left join silver.olist.item_pedido as t2
on t1.idPedido = t2.idPedido
where dtPedido >= '2018-01-01'
and t1.dtPedido <= date_add('2018-01-01', 45)
and idVendedor is not null

group by 1
)

select 
t1.*,
t2.*,
t3.*,
t4.*,
t5.*,
t6.*,
t7.*.






, case when t7.idVendedor is null then 1 else 0 end as flChurn

    FROM silver.analytics.fs_vendedor_vendas AS t1

    LEFT JOIN silver.analytics.fs_vendedor_avaliacao AS t2
    ON t1.idVendedor = t2.idVendedor
    AND t1.dtReference = t2.dtReference

    LEFT JOIN silver.analytics.fs_vendedor_cliente AS t3
    ON t1.idVendedor = t3.idVendedor
    AND t1.dtReference = t3.dtReference

    LEFT JOIN silver.analytics.fs_vendedor_entrega AS t4
    ON t1.idVendedor = t4.idVendedor
    AND t1.dtReference = t4.dtReference

    LEFT JOIN silver.analytics.fs_vendedor_pagamentos AS t5
    ON t1.idVendedor = t5.idVendedor
    AND t1.dtReference = t5.dtReference

    LEFT JOIN silver.analytics.fs_vendedor_produto AS t6
    ON t1.idVendedor = t6.idVendedor
    AND t1.dtReference = t6.dtReference

    WHERE t1.qtdRecencia <= 45

left join tb_activate as t7
on t1.idVendedor = t7.idVendedor
and datediff(t7.dtAtivacao, t1.dtReference) + qtdRecencia <= 45

where qtdRecencia <= 45

-- COMMAND ----------


