-- Databricks notebook source
with tb_pedido as (
SELECT t1.idPedido
	,t2.idVendedor
	,t1.descSituacao
	,t1.dtPedido
	,t1.dtAprovado
	,t1.dtEntregue
	,t1.dtEstimativaEntrega
	,sum(vlFrete) AS totalFrete
FROM silver.olist.pedido AS t1
LEFT JOIN silver.olist.item_pedido AS t2 ON t1.idPedido = t2.idPedido
WHERE dtPedido < '2018-01-01'
	AND dtPedido >= add_months('2018-01-01', - 6)
GROUP BY t1.idPedido
	,t2.idVendedor
	,t1.descSituacao
	,t1.dtPedido
	,t1.dtAprovado
	,t1.dtEntregue
	,t1.dtEstimativaEntrega
)

SELECT idVendedor
	,count(DISTINCT CASE 
			WHEN descSituacao = 'delivered'
				AND DATE (coalesce(dtEntregue, '2018-01-01')) > DATE (dtEstimativaEntrega)
				THEN idPedido
			END) / count(DISTINCT CASE 
			WHEN descSituacao = 'delivered'
				THEN idPedido
			END) AS pctPedidoAtraso
	,count(DISTINCT CASE 
			WHEN descSituacao = 'canceled'
				THEN idPedido
			END) / count(DISTINCT idPedido) AS pctPedidoCancelado,
            avg(totalFrete) as avgFrete,
            percentile(totalFrete, 0.5) as medianFrete,
            max(totalFrete) as maxFrete,
            min(totalFrete) as minFrete,
            avg(datediff(coalesce(dtEntregue, '2018-01-01'), dtAprovado)) as qtdDiasAprovadoEntrega,
            avg(datediff(coalesce(dtEntregue, '2018-01-01'), dtPedido)) as qtdDiasPedidoEntrega,
            avg(datediff(coalesce(dtEntregue, '2018-01-01'), coalesce(dtEstimativaEntrega, '2018-01-01'))) as qtdDiasEntreguePromessa
FROM tb_pedido
GROUP BY 1

-- COMMAND ----------


