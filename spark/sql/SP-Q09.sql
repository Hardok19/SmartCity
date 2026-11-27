-- SP-Q09: Comparacion historica entre 2023 y 2024 (PIVOT por anio)
-- PM2.5 por zona y origen de los datos.
-- Incluye conteos para comprobar la cobertura anual.

WITH base AS (
    SELECT zona, origen, anio, valor
    FROM ambiente
    WHERE tipo_metrica = 'pm25'
      AND anio IN (2023, 2024)
      AND valor IS NOT NULL
),

comparacion AS (
    SELECT *
    FROM base
    PIVOT (
        COUNT(valor) AS mediciones,
        AVG(valor)   AS pm25
        FOR anio IN (2023 AS a2023, 2024 AS a2024)
    )
)

SELECT
    zona,
    origen,
    COALESCE(a2023_mediciones, 0) AS mediciones_2023,
    ROUND(a2023_pm25, 2)          AS pm25_2023,
    COALESCE(a2024_mediciones, 0) AS mediciones_2024,
    ROUND(a2024_pm25, 2)          AS pm25_2024,

    ROUND(a2024_pm25 - a2023_pm25, 2)
        AS diferencia_absoluta,

    ROUND(
        100.0 * (a2024_pm25 - a2023_pm25)
        / NULLIF(a2023_pm25, 0),
        2
    ) AS variacion_porcentual

FROM comparacion

ORDER BY zona, origen
