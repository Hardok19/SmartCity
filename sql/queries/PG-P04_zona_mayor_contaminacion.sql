-- id: PG-P04
-- titulo: ¿Cuál zona presenta mayor contaminación?
-- tecnicas: Vista materializada, AVG() OVER (ROWS BETWEEN 6 PRECEDING) = media móvil 7 días, LAG(), RANK()
-- parametros: desde=2024-01-01; hasta=2024-12-31
WITH diario AS (
    SELECT zona, fecha, pm25_promedio,
           AVG(pm25_promedio) OVER (PARTITION BY zona ORDER BY fecha
                                    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)         AS media_movil_7d,
           pm25_promedio - LAG(pm25_promedio) OVER (PARTITION BY zona ORDER BY fecha) AS cambio_vs_dia_anterior
    FROM mv_kpi_diario_zona
    WHERE fecha BETWEEN %(desde)s::date AND %(hasta)s::date AND pm25_promedio IS NOT NULL
)
SELECT RANK() OVER (ORDER BY AVG(pm25_promedio) DESC)      AS ranking,
       zona,
       ROUND(AVG(pm25_promedio)::numeric, 2)                AS pm25_promedio,
       ROUND(MAX(media_movil_7d)::numeric, 2)               AS pico_media_movil_7d,
       ROUND(MAX(cambio_vs_dia_anterior)::numeric, 2)       AS mayor_subida_diaria
FROM diario
GROUP BY zona
ORDER BY ranking;
