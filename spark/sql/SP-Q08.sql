-- SP-Q08: Agrupamiento por categorias de sensores
-- JOIN entre las mediciones de HDFS y el catalogo maestro.
-- ROLLUP permite obtener detalles, subtotales y total general.

WITH datos AS (
    SELECT
        a.id_sensor,
        a.zona,
        a.tipo_metrica,
        COALESCE(c.categoria, 'Sin catalogo') AS categoria_maestra,
        c.id_sensor AS id_catalogo,
        c.zona AS zona_catalogo
    FROM ambiente a
    LEFT JOIN catalogo_sensores c
        ON a.id_sensor = c.id_sensor
)
SELECT
    CASE
        WHEN GROUPING(categoria_maestra) = 1
        THEN 'TOTAL GENERAL'
        ELSE categoria_maestra
    END AS categoria,

    CASE
        WHEN GROUPING(tipo_metrica) = 1
        THEN 'TODAS'
        ELSE tipo_metrica
    END AS indicador,

    CASE
        WHEN GROUPING(zona) = 1
        THEN 'TODAS'
        ELSE zona
    END AS zona,

    COUNT(*) AS total_mediciones,
    COUNT(DISTINCT id_sensor) AS sensores_activos,

    COUNT(DISTINCT CASE
        WHEN id_catalogo IS NULL THEN id_sensor
    END) AS sensores_sin_catalogo,

    SUM(CASE
        WHEN id_catalogo IS NOT NULL
         AND zona <> zona_catalogo THEN 1
        ELSE 0
    END) AS mediciones_zona_inconsistente,

    GROUPING_ID(
        categoria_maestra,
        tipo_metrica,
        zona
    ) AS nivel_agrupacion

FROM datos
GROUP BY ROLLUP(
    categoria_maestra,
    tipo_metrica,
    zona
)
ORDER BY
    nivel_agrupacion DESC,
    total_mediciones DESC
LIMIT 40
