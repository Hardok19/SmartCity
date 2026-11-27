-- SP-Q05: Ranking de zonas segun cantidad de mediciones
WITH actividad AS (
    SELECT
        zona,
        COUNT(*) AS mediciones,
        COUNT(DISTINCT id_sensor) AS sensores_activos
    FROM ambiente
    GROUP BY zona
)
SELECT
    RANK() OVER (ORDER BY mediciones DESC) AS ranking,
    zona,
    mediciones,
    sensores_activos
FROM actividad
ORDER BY ranking
