-- SP-Q09: Comparacion historica entre 2023 y 2024
-- PM2.5 por zona y origen de los datos.
-- Incluye conteos para comprobar la cobertura anual.

WITH comparacion AS (
    SELECT
        zona,
        origen,

        COUNT(CASE WHEN anio = 2023 THEN 1 END)
            AS mediciones_2023,

        ROUND(AVG(CASE WHEN anio = 2023 THEN valor END), 2)
            AS pm25_2023,

        COUNT(CASE WHEN anio = 2024 THEN 1 END)
            AS mediciones_2024,

        ROUND(AVG(CASE WHEN anio = 2024 THEN valor END), 2)
            AS pm25_2024

    FROM ambiente

    WHERE tipo_metrica = 'pm25'
      AND anio IN (2023, 2024)
      AND valor IS NOT NULL

    GROUP BY zona, origen
)

SELECT
    zona,
    origen,
    mediciones_2023,
    pm25_2023,
    mediciones_2024,
    pm25_2024,

    ROUND(pm25_2024 - pm25_2023, 2)
        AS diferencia_absoluta,

    ROUND(
        100.0 * (pm25_2024 - pm25_2023)
        / NULLIF(pm25_2023, 0),
        2
    ) AS variacion_porcentual

FROM comparacion

ORDER BY zona, origen
