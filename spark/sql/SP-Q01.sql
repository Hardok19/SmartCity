-- SP-Q01: Sensores que generan mas informacion
SELECT
    id_sensor,
    COUNT(*) AS total_mediciones,
    COUNT(DISTINCT tipo_metrica) AS variables
FROM ambiente
GROUP BY id_sensor
ORDER BY total_mediciones DESC
LIMIT 10
