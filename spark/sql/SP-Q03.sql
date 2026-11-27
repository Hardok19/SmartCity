-- SP-Q03: Evolucion temporal de indicadores urbanos
-- Promedios mensuales por zona, variable y origen de datos.
-- Se utilizan las mediciones depuradas del ETL.
-- Cada indicador se analiza por separado.

WITH base AS (
    SELECT
        date_trunc('month', timestamp) AS mes_inicio,
        zona,
        tipo_metrica AS indicador,
        origen,
        id_sensor,
        valor
    FROM ambiente
    WHERE timestamp IS NOT NULL
      AND zona IS NOT NULL
      AND tipo_metrica IS NOT NULL
      AND valor IS NOT NULL
),
evolucion AS (
    SELECT
        mes_inicio,
        zona,
        indicador,
        origen,
        COUNT(*) AS total_mediciones,
        COUNT(DISTINCT id_sensor) AS sensores_activos,
        ROUND(AVG(valor), 2) AS promedio,
        ROUND(MIN(valor), 2) AS minimo,
        ROUND(MAX(valor), 2) AS maximo
    FROM base
    GROUP BY
        mes_inicio,
        zona,
        indicador,
        origen
)
SELECT
    date_format(mes_inicio, 'yyyy-MM') AS mes,
    zona,
    indicador,
    origen,
    total_mediciones,
    sensores_activos,
    promedio,
    minimo,
    maximo
FROM evolucion
ORDER BY
    mes_inicio,
    zona,
    indicador,
    origen
