-- id: PG-Q08
-- titulo: Ranking de zonas monitoreadas
-- tecnicas: CTE, RANK() OVER, SUM() OVER (porcentaje del total), vista materializada
-- parametros: 
WITH por_zona AS (
    SELECT u.nombre_zona                                            AS zona,
           COUNT(DISTINCT s.id_sensor)                              AS sensores,
           COUNT(DISTINCT s.id_sensor) FILTER (WHERE s.estado = 'activo') AS activos,
           COALESCE(SUM(a.num_mediciones), 0)                       AS mediciones
    FROM ubicaciones u
    LEFT JOIN sensores s            USING (id_ubicacion)
    LEFT JOIN mv_actividad_sensor a USING (id_sensor)
    GROUP BY u.nombre_zona
)
SELECT RANK() OVER (ORDER BY mediciones DESC, sensores DESC)               AS ranking,
       zona, sensores, activos, mediciones,
       ROUND(100.0 * mediciones / NULLIF(SUM(mediciones) OVER (), 0), 2)  AS pct_mediciones
FROM por_zona
ORDER BY ranking;
