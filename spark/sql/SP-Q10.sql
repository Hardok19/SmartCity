-- SP-Q10: Reporte consolidado de indicadores ambientales por zona
-- Fuentes externas y sinteticas separadas: no mezclar sus promedios.
-- Usa la vista ambiente creada por ejecutar_consultas.py desde HDFS.

SELECT
    zona,
    COUNT(*) AS total_mediciones,
    COUNT(DISTINCT id_sensor) AS sensores_activos,

    COUNT_IF(origen = 'openaq') AS mediciones_openaq,
    COUNT_IF(origen = 'open-meteo') AS mediciones_openmeteo,
    COUNT_IF(origen = 'sintetico') AS mediciones_sinteticas,
    COUNT_IF(origen IS NULL OR origen NOT IN (
        'openaq', 'open-meteo', 'sintetico'
    )) AS mediciones_origen_desconocido,

    COUNT_IF(tipo_metrica = 'pm25' AND origen = 'openaq')
        AS muestras_pm25_openaq,
    ROUND(AVG(CASE
        WHEN tipo_metrica = 'pm25' AND origen = 'openaq'
        THEN valor END), 2) AS pm25_promedio_openaq,
    ROUND(AVG(CASE
        WHEN tipo_metrica = 'pm25' AND origen = 'sintetico'
        THEN valor END), 2) AS pm25_promedio_sintetico,

    ROUND(AVG(CASE
        WHEN tipo_metrica = 'temperatura' AND origen = 'open-meteo'
        THEN valor END), 2) AS temperatura_promedio_openmeteo,
    ROUND(AVG(CASE
        WHEN tipo_metrica = 'humedad' AND origen = 'open-meteo'
        THEN valor END), 2) AS humedad_promedio_openmeteo,
    ROUND(AVG(CASE
        WHEN tipo_metrica = 'co' AND origen = 'openaq'
        THEN valor END), 2) AS co_promedio_openaq,

    MIN(timestamp) AS fecha_inicio,
    MAX(timestamp) AS fecha_fin
FROM ambiente
GROUP BY zona
ORDER BY zona
