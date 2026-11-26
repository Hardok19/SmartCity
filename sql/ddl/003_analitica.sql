CREATE INDEX IF NOT EXISTS idx_sensores_organismo ON sensores(id_organismo);
CREATE INDEX IF NOT EXISTS idx_sensores_fecha ON sensores(fecha_instalacion);
CREATE INDEX IF NOT EXISTS idx_medicion_sensor_fecha ON medicion_horaria(id_sensor,timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_medicion_fecha_brin ON medicion_horaria USING BRIN(timestamp);
CREATE INDEX IF NOT EXISTS idx_medicion_variable_fecha ON medicion_horaria(variable,timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_sensores_activos_tipo ON sensores(tipo_sensor) WHERE estado='activo';
CREATE OR REPLACE VIEW v_sensor_detalle AS
SELECT s.id_sensor,s.nombre,s.tipo_sensor,s.estado,s.fecha_instalacion,s.origen,
       s.envia_datos,s.fecha_baja,u.nombre_zona AS zona,u.distrito,u.latitud AS lat,
       u.longitud AS lon,c.nombre_categoria AS categoria,o.nombre AS organismo
FROM sensores s
LEFT JOIN ubicaciones u ON u.id_ubicacion=s.id_ubicacion
LEFT JOIN categorias_sensor c ON c.id_categoria=s.id_categoria
LEFT JOIN organismos o ON o.id_organismo=s.id_organismo;
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_actividad_sensor AS
SELECT s.id_sensor, COUNT(m.timestamp) AS num_mediciones,
MIN(m.timestamp) AS primera_lectura, MAX(m.timestamp) AS ultima_lectura
FROM sensores s LEFT JOIN medicion_horaria m ON m.id_sensor=s.id_sensor
GROUP BY s.id_sensor WITH DATA;
CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_actividad_sensor_pk ON mv_actividad_sensor(id_sensor);
CREATE OR REPLACE FUNCTION fn_sensores_por_organismo()
RETURNS TABLE(organismo TEXT,cantidad BIGINT) LANGUAGE sql STABLE AS $$
 SELECT o.nombre::text, COUNT(s.id_sensor) FROM organismos o
 LEFT JOIN sensores s ON s.id_organismo=o.id_organismo
 GROUP BY o.id_organismo,o.nombre ORDER BY COUNT(s.id_sensor) DESC,o.nombre;
$$;
CREATE OR REPLACE FUNCTION fn_sensores_sin_actividad(p_desde TIMESTAMPTZ)
RETURNS TABLE(id_sensor VARCHAR,nombre VARCHAR,ultima_lectura TIMESTAMPTZ)
LANGUAGE sql STABLE AS $$
 SELECT s.id_sensor,s.nombre,MAX(m.timestamp)
 FROM sensores s LEFT JOIN medicion_horaria m ON m.id_sensor=s.id_sensor
 GROUP BY s.id_sensor,s.nombre
 HAVING MAX(m.timestamp) IS NULL OR MAX(m.timestamp) < p_desde
 ORDER BY s.id_sensor;
$$;
