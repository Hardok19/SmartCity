-- SP-Q02: Distribucion geografica de eventos de MongoDB.
-- Una fila por zona, separando tipos y severidades.
-- Vista 'eventos': coleccion MongoDB smartcity_db.evento.
-- Vista 'zonas': cinco zonas presentes en el ETL de HDFS.
WITH resumen AS (
    SELECT
        z.zona,
        COUNT(e.tipo_evento) AS total_eventos,
        COUNT_IF(e.tipo_evento = 'contaminacion_alta') AS contaminacion_alta,
        COUNT_IF(e.tipo_evento = 'falla_sensor') AS fallas_sensor,
        COUNT_IF(e.tipo_evento = 'lluvia_intensa') AS lluvias_intensas,
        COUNT_IF(e.tipo_evento = 'ola_calor') AS olas_calor,
        COUNT_IF(e.tipo_evento IS NOT NULL AND e.tipo_evento NOT IN (
            'contaminacion_alta', 'falla_sensor', 'lluvia_intensa', 'ola_calor'
        )) AS otros_eventos,
        COUNT_IF(e.severidad = 'baja') AS severidad_baja,
        COUNT_IF(e.severidad = 'media') AS severidad_media,
        COUNT_IF(e.severidad = 'alta') AS severidad_alta,
        COUNT_IF(e.severidad = 'critica') AS severidad_critica,
        ROUND(MAX(e.latitud), 5) AS latitud,
        ROUND(MAX(e.longitud), 5) AS longitud
    FROM zonas z
    LEFT JOIN eventos e ON e.zona = z.zona
    GROUP BY z.zona
)
SELECT
    zona,
    total_eventos,
    ROUND(100.0 * total_eventos /
        NULLIF(SUM(total_eventos) OVER (), 0), 2) AS porcentaje_ciudad,
    contaminacion_alta,
    fallas_sensor,
    lluvias_intensas,
    olas_calor,
    otros_eventos,
    severidad_baja,
    severidad_media,
    severidad_alta,
    severidad_critica,
    latitud,
    longitud,
    SUM(total_eventos) OVER () AS total_ciudad
FROM resumen
ORDER BY total_eventos DESC, zona
