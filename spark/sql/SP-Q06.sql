-- SP-Q06: Tendencia mensual PM2.5; variacion porcentual respecto al mes anterior
WITH mensual AS (
    SELECT
        zona,
        date_trunc('month', timestamp) AS mes,
        AVG(valor) AS pm25_promedio
    FROM ambiente
    WHERE tipo_metrica = 'pm25'
    GROUP BY zona, date_trunc('month', timestamp)
), tendencias AS (
    SELECT
        zona,
        mes,
        pm25_promedio,
        LAG(pm25_promedio) OVER (PARTITION BY zona ORDER BY mes) AS mes_anterior
    FROM mensual
)
SELECT
    zona,
    date_format(mes, 'yyyy-MM') AS mes,
    ROUND(pm25_promedio, 2) AS pm25_promedio,
    ROUND(mes_anterior, 2) AS pm25_mes_anterior,
    CASE
        WHEN mes_anterior IS NULL OR mes_anterior = 0 THEN NULL
        ELSE ROUND(100.0 * (pm25_promedio - mes_anterior) / mes_anterior, 2)
    END AS variacion_pct
FROM tendencias
ORDER BY zona, mes
