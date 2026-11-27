-- id: PG-Q01
-- titulo: Sensores por tipo
-- tecnicas: JOIN, GROUP BY, COUNT … FILTER, ORDER BY
-- parametros: 
SELECT c.nombre_categoria                          AS categoria,
       s.tipo_sensor,
       COUNT(*)                                    AS sensores,
       COUNT(*) FILTER (WHERE s.estado = 'activo') AS activos
FROM sensores s
JOIN tipos_sensor t      USING (tipo_sensor)
JOIN categorias_sensor c USING (id_categoria)
GROUP BY c.nombre_categoria, s.tipo_sensor
ORDER BY sensores DESC, s.tipo_sensor;
