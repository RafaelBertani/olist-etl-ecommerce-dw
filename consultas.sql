-- =================================================================
-- SCRIPT PARA REALIZAR AS CONSULTAS ANALÍTICAS
-- =================================================================

\connect P4_olist

-- Total de vendas por ano, mês e estado — com ROLLUP
SELECT
    t.ano,
    t.mes,
    l.geolocation_state AS estado,
    SUM(f.valor_venda) AS total_vendas
FROM datamart.fatoVendas f
JOIN datamart.dimTempo t ON f.sk_tempo = t.sk_tempo
JOIN datamart.dimLocalidade l ON f.sk_localidade = l.sk_localidade
GROUP BY ROLLUP (t.ano, t.mes, l.geolocation_state)
ORDER BY t.ano, t.mes, l.geolocation_state
LIMIT 50; -- Limitamos a 50 para, caso este script seja executado por linha de comando, não inviabilize a visibilidade dos demais resultados de outras análises.

-- Total de vendas por categoria de produto, estado e ano — com CUBE
SELECT
    p.product_category_name AS categoria,
    l.geolocation_state AS estado,
    t.ano,
    SUM(f.valor_venda) AS total_vendas
FROM datamart.fatoVendas f
JOIN datamart.dimProduto p ON f.sk_produto = p.sk_produto
JOIN datamart.dimLocalidade l ON f.sk_localidade = l.sk_localidade
JOIN datamart.dimTempo t ON f.sk_tempo = t.sk_tempo
GROUP BY CUBE (p.product_category_name, l.geolocation_state, t.ano)
ORDER BY categoria, estado, t.ano
LIMIT 50; -- Limitamos a 50 para, caso este script seja executado por linha de comando, não inviabilize a visibilidade dos demais resultados de outras análises.

-- Top 10 clientes com maior valor total de compras — com RANK
SELECT
    c.customer_unique_id AS cliente,
    SUM(f.valor_venda) AS total_compras,
    RANK() OVER (ORDER BY SUM(f.valor_venda) DESC) AS posicao_rank
FROM datamart.fatoVendas f
JOIN datamart.dimCliente c ON f.sk_cliente = c.sk_cliente
GROUP BY c.customer_unique_id
ORDER BY total_compras DESC
LIMIT 10;

-- Média de vendas mensal por estado e categoria — com DENSE_RANK e FIRST_VALUE
WITH vendas_mensais AS (
    SELECT
        l.geolocation_state AS estado,
        p.product_category_name AS categoria,
        t.ano,
        t.mes,
        SUM(f.valor_venda) AS total_vendas
    FROM datamart.fatoVendas f
    JOIN datamart.dimLocalidade l ON f.sk_localidade = l.sk_localidade
    JOIN datamart.dimProduto p ON f.sk_produto = p.sk_produto
    JOIN datamart.dimTempo t ON f.sk_tempo = t.sk_tempo
    GROUP BY l.geolocation_state, p.product_category_name, t.ano, t.mes
)
SELECT
    estado,
    categoria,
    ano,
    mes,
    total_vendas,
    DENSE_RANK() OVER (PARTITION BY estado ORDER BY total_vendas DESC) AS rank_estado,
    FIRST_VALUE(mes) OVER (PARTITION BY estado ORDER BY total_vendas DESC) AS melhor_mes
FROM vendas_mensais
ORDER BY estado, rank_estado
LIMIT 50; -- Limitamos a 50 para, caso este script seja executado por linha de comando, não inviabilize a visibilidade dos demais resultados de outras análises.

-- Evolução mensal do valor médio de vendas — com ROW_NUMBER, LEAD e LAG
WITH media_mensal AS (
    SELECT
        t.ano,
        t.mes,
        ROUND(AVG(f.valor_venda), 2) AS media_venda
    FROM datamart.fatoVendas f
    JOIN datamart.dimTempo t ON f.sk_tempo = t.sk_tempo
    GROUP BY t.ano, t.mes
)
SELECT
    ROW_NUMBER() OVER (ORDER BY ano, mes) AS id_linha,
    ano,
    mes,
    media_venda,
    LAG(media_venda, 1) OVER (ORDER BY ano, mes) AS media_anterior,
    LEAD(media_venda, 1) OVER (ORDER BY ano, mes) AS media_proxima,
    ROUND(media_venda - LAG(media_venda, 1) OVER (ORDER BY ano, mes), 2) AS variacao_mes
FROM media_mensal
ORDER BY ano, mes
LIMIT 50; -- Limitamos a 50 para, caso este script seja executado por linha de comando, não inviabilize a visibilidade dos demais resultados de outras análises.
