-- 003_analitica.sql — A2.1 vistas y vistas materializadas, A2.2 funciones y procedimientos. Re-ejecutable.

-- ---------------------------------------------------------------- vistas
-- Sensor con su tipo, categoría, ubicación y organismo (evita repetir 4 JOIN en cada consulta).
CREATE OR REPLACE VIEW v_sensor_detalle AS
SELECT s.id_sensor, s.nombre, s.tipo_sensor, s.estado, s.fecha_instalacion, s.origen,
       s.envia_datos, s.fecha_baja, u.nombre_zona AS zona, u.distrito,
       COALESCE(s.latitud, u.latitud) AS lat, COALESCE(s.longitud, u.longitud) AS lon,
       c.nombre_categoria AS categoria, o.nombre AS organismo
FROM sensores s
LEFT JOIN tipos_sensor t      ON t.tipo_sensor = s.tipo_sensor
LEFT JOIN categorias_sensor c ON c.id_categoria = t.id_categoria
LEFT JOIN ubicaciones u       ON u.id_ubicacion = s.id_ubicacion
LEFT JOIN organismos o        ON o.id_organismo = s.id_organismo;
COMMENT ON VIEW v_sensor_detalle IS 'Sensor + tipo + categoría + ubicación + organismo (PG-Q09)';

-- Actividad por sensor: nº de mediciones, primera y última lectura.
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_actividad_sensor AS
SELECT s.id_sensor, COUNT(m.timestamp) AS num_mediciones,
       MIN(m.timestamp) AS primera_lectura, MAX(m.timestamp) AS ultima_lectura
FROM sensores s LEFT JOIN medicion_horaria m ON m.id_sensor = s.id_sensor
GROUP BY s.id_sensor
WITH DATA;
CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_actividad_sensor_pk ON mv_actividad_sensor(id_sensor);

-- KPI diario por zona (PM2.5, temperatura, humedad). Precalculado: el dashboard y fn_kpi_ambiente lo leen
-- sin recorrer medicion_horaria. Índice UNIQUE para poder usar REFRESH … CONCURRENTLY.
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_kpi_diario_zona AS
SELECT u.nombre_zona AS zona,
       (m.timestamp AT TIME ZONE 'UTC')::date AS fecha,
       AVG(m.valor) FILTER (WHERE m.variable = 'pm25')        AS pm25_promedio,
       AVG(m.valor) FILTER (WHERE m.variable = 'temperatura') AS temperatura_promedio,
       AVG(m.valor) FILTER (WHERE m.variable = 'humedad')     AS humedad_promedio,
       COUNT(*) AS mediciones
FROM medicion_horaria m
JOIN sensores s    USING (id_sensor)
JOIN ubicaciones u USING (id_ubicacion)
GROUP BY u.nombre_zona, (m.timestamp AT TIME ZONE 'UTC')::date
WITH DATA;
CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_kpi_diario_zona_pk ON mv_kpi_diario_zona(zona, fecha);

-- ---------------------------------------------------------------- funciones
CREATE OR REPLACE FUNCTION fn_sensores_por_organismo()
RETURNS TABLE(organismo TEXT, cantidad BIGINT) LANGUAGE sql STABLE AS $$
    SELECT o.nombre::text, COUNT(s.id_sensor)
    FROM organismos o
    LEFT JOIN sensores s ON s.id_organismo = o.id_organismo
    GROUP BY o.id_organismo, o.nombre
    ORDER BY COUNT(s.id_sensor) DESC, o.nombre;
$$;

-- Sensores sin actividad desde p_desde, distinguiendo los que NUNCA enviaron de los que DEJARON de enviar.
DROP FUNCTION IF EXISTS fn_sensores_sin_actividad(TIMESTAMPTZ);
CREATE FUNCTION fn_sensores_sin_actividad(p_desde TIMESTAMPTZ)
RETURNS TABLE(id_sensor VARCHAR, nombre VARCHAR, zona VARCHAR, ultima_lectura TIMESTAMPTZ, situacion TEXT)
LANGUAGE sql STABLE AS $$
    SELECT s.id_sensor, s.nombre, u.nombre_zona, a.ultima_lectura,
           CASE WHEN a.ultima_lectura IS NULL THEN 'nunca envió datos' ELSE 'dejó de enviar' END
    FROM sensores s
    JOIN ubicaciones u USING (id_ubicacion)
    LEFT JOIN mv_actividad_sensor a USING (id_sensor)
    WHERE a.ultima_lectura IS NULL OR a.ultima_lectura < p_desde
    ORDER BY a.ultima_lectura NULLS FIRST, s.id_sensor;
$$;

-- KPI de ambiente por zona en un período (todas las zonas si p_zona es NULL).
CREATE OR REPLACE FUNCTION fn_kpi_ambiente(p_desde DATE, p_hasta DATE, p_zona TEXT DEFAULT NULL)
RETURNS TABLE(zona TEXT, dias BIGINT, pm25_promedio NUMERIC, temperatura_promedio NUMERIC,
              humedad_promedio NUMERIC, mediciones BIGINT)
LANGUAGE sql STABLE AS $$
    SELECT k.zona::text, COUNT(*), ROUND(AVG(k.pm25_promedio)::numeric, 2),
           ROUND(AVG(k.temperatura_promedio)::numeric, 2), ROUND(AVG(k.humedad_promedio)::numeric, 2),
           SUM(k.mediciones)::bigint
    FROM mv_kpi_diario_zona k
    WHERE k.fecha BETWEEN p_desde AND p_hasta
      AND (p_zona IS NULL OR k.zona = p_zona)
    GROUP BY k.zona
    ORDER BY k.zona;
$$;

-- Refresca las vistas materializadas sin bloquear lecturas (las llama spark/jobs/carga_pg.py al final).
CREATE OR REPLACE PROCEDURE sp_refrescar_mv()
LANGUAGE plpgsql AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_actividad_sensor;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_kpi_diario_zona;
END $$;
