-- SP-Q04: Correlacion entre PM2.5 y variables climaticas
-- Se analizan por separado fuentes externas y datos sinteticos.
-- Primero se agregan las lecturas por zona y hora.

WITH base AS (
    SELECT
        zona,
        date_trunc('hour', timestamp) AS hora,
        CASE
            WHEN origen = 'sintetico' THEN 'sintetico'
            ELSE 'fuentes_externas'
        END AS grupo_fuente,
        tipo_metrica,
        valor
    FROM ambiente
    WHERE timestamp IS NOT NULL
      AND valor IS NOT NULL
      AND tipo_metrica IN (
          'pm25', 'temperatura', 'humedad',
          'viento', 'precipitacion'
      )
      AND (
          origen = 'sintetico'
          OR (origen = 'openaq' AND tipo_metrica = 'pm25')
          OR (
              origen = 'open-meteo'
              AND tipo_metrica IN (
                  'temperatura', 'humedad',
                  'viento', 'precipitacion'
              )
          )
      )
),
por_hora AS (
    SELECT
        zona,
        hora,
        grupo_fuente,
        AVG(CASE WHEN tipo_metrica = 'pm25'
            THEN valor END) AS pm25,
        AVG(CASE WHEN tipo_metrica = 'temperatura'
            THEN valor END) AS temperatura,
        AVG(CASE WHEN tipo_metrica = 'humedad'
            THEN valor END) AS humedad,
        AVG(CASE WHEN tipo_metrica = 'viento'
            THEN valor END) AS viento,
        AVG(CASE WHEN tipo_metrica = 'precipitacion'
            THEN valor END) AS precipitacion
    FROM base
    GROUP BY zona, hora, grupo_fuente
)
SELECT
    zona,
    grupo_fuente,
    COUNT(CASE
        WHEN pm25 IS NOT NULL AND temperatura IS NOT NULL
        THEN 1 END) AS pares_temperatura,
    ROUND(CORR(pm25, temperatura), 3) AS corr_temperatura,
    COUNT(CASE
        WHEN pm25 IS NOT NULL AND humedad IS NOT NULL
        THEN 1 END) AS pares_humedad,
    ROUND(CORR(pm25, humedad), 3) AS corr_humedad,
    COUNT(CASE
        WHEN pm25 IS NOT NULL AND viento IS NOT NULL
        THEN 1 END) AS pares_viento,
    ROUND(CORR(pm25, viento), 3) AS corr_viento,
    COUNT(CASE
        WHEN pm25 IS NOT NULL AND precipitacion IS NOT NULL
        THEN 1 END) AS pares_precipitacion,
    ROUND(CORR(pm25, precipitacion), 3) AS corr_precipitacion
FROM por_hora
GROUP BY zona, grupo_fuente
ORDER BY zona, grupo_fuente
