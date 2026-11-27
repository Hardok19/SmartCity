-- id: PG-Q07
-- titulo: Sensores sin actividad registrada
-- tecnicas: NOT EXISTS (usa idx_medicion_sensor_fecha), JOIN
-- parametros: 
SELECT s.id_sensor, s.nombre, u.nombre_zona AS zona, s.tipo_sensor, s.estado, s.envia_datos
FROM sensores s
JOIN ubicaciones u USING (id_ubicacion)
WHERE NOT EXISTS (SELECT 1 FROM medicion_horaria m WHERE m.id_sensor = s.id_sensor)
ORDER BY s.id_sensor;
-- Equivalente con LEFT JOIN … IS NULL:
--   SELECT s.id_sensor FROM sensores s LEFT JOIN medicion_horaria m USING (id_sensor) WHERE m.id_sensor IS NULL;
