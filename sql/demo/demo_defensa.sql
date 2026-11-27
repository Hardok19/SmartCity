-- demo_defensa.sql — A2.6. Plan B de la defensa si la interfaz falla: recorrido completo en psql.
--   docker exec -it smartcity_postgres psql -U smartcity -d smartcity_db -f /dev/stdin < sql/demo/demo_defensa.sql
-- o, dentro de psql:  \i sql/demo/demo_defensa.sql   (con \timing on se ve el tiempo de cada consulta)
\pset pager off
\timing on

\echo '=== 1. Modelo: tablas, filas y particiones ==='
SELECT relname AS tabla, n_live_tup AS filas_aprox
FROM pg_stat_user_tables
WHERE relname IN ('ubicaciones','categorias_sensor','tipos_sensor','organismos','sensores','usuarios',
                  'variables_medicion','calendario','medicion_horaria_default','agg_ambiente_hora_zona',
                  'spark_kpi_zona','spark_anomalia')
ORDER BY relname;
SELECT COUNT(*) AS particiones_mensuales FROM pg_inherits i JOIN pg_class p ON p.oid = i.inhparent
WHERE p.relname = 'medicion_horaria';
SELECT COUNT(*) AS mediciones_horarias FROM medicion_horaria;

\echo '=== 2. Integridad: una FK y un CHECK rechazan datos inválidos ==='
DO $$ BEGIN
    INSERT INTO sensores (id_sensor, nombre, tipo_sensor, id_ubicacion) VALUES ('S-999', 'x', 'tipo_inexistente', 1);
EXCEPTION WHEN foreign_key_violation THEN RAISE NOTICE 'FK OK: % ', SQLERRM; END $$;
DO $$ BEGIN
    INSERT INTO usuarios (nombre, correo) VALUES ('Prueba', 'correo-sin-arroba');
EXCEPTION WHEN check_violation THEN RAISE NOTICE 'CHECK OK: %', SQLERRM; END $$;
DO $$ BEGIN
    DELETE FROM organismos WHERE nombre = 'Met Office';
EXCEPTION WHEN foreign_key_violation THEN RAISE NOTICE 'ON DELETE RESTRICT OK: %', SQLERRM; END $$;

\echo '=== 3. Consultas obligatorias (selección) ==='
-- PG-Q03 activos vs inactivos (FILTER + ROLLUP)
SELECT COALESCE(u.nombre_zona, 'TOTAL') AS zona, COUNT(*) FILTER (WHERE s.estado = 'activo') AS activos,
       COUNT(*) FILTER (WHERE s.estado = 'inactivo') AS inactivos
FROM sensores s JOIN ubicaciones u USING (id_ubicacion) GROUP BY ROLLUP (u.nombre_zona)
ORDER BY GROUPING(u.nombre_zona), zona;
-- PG-Q08 ranking con CTE + RANK
WITH por_zona AS (
    SELECT u.nombre_zona AS zona, COUNT(DISTINCT s.id_sensor) AS sensores, COALESCE(SUM(a.num_mediciones), 0) AS mediciones
    FROM ubicaciones u LEFT JOIN sensores s USING (id_ubicacion) LEFT JOIN mv_actividad_sensor a USING (id_sensor)
    GROUP BY u.nombre_zona)
SELECT RANK() OVER (ORDER BY mediciones DESC) AS ranking, * FROM por_zona ORDER BY ranking;
-- PG-Q09 vista y PG-Q10 función
SELECT zona, categoria, COUNT(*) AS sensores FROM v_sensor_detalle GROUP BY zona, categoria ORDER BY zona, categoria LIMIT 10;
SELECT * FROM fn_kpi_ambiente('2024-01-01', '2024-12-31');
SELECT * FROM fn_sensores_por_organismo();

\echo '=== 4. Preguntas del enunciado ==='
-- ¿Qué sensores han dejado de enviar información?
SELECT * FROM fn_sensores_sin_actividad('2024-12-01') LIMIT 10;
-- ¿Cuál es la hora de mayor congestión? (laborables, hora local, con calendario)
SELECT EXTRACT(HOUR FROM m.timestamp AT TIME ZONE 'Europe/London')::int AS hora_local, ROUND(AVG(m.valor)) AS vehiculos_hora
FROM medicion_horaria m JOIN calendario c ON c.fecha = (m.timestamp AT TIME ZONE 'Europe/London')::date
WHERE m.variable = 'trafico' AND NOT c.es_fin_de_semana AND NOT c.es_feriado
GROUP BY 1 ORDER BY vehiculos_hora DESC LIMIT 3;
-- ¿Cuál zona presenta mayor contaminación? (media móvil 7 días con ventanas)
SELECT zona, ROUND(AVG(pm25_promedio)::numeric, 2) AS pm25_promedio FROM mv_kpi_diario_zona
WHERE pm25_promedio IS NOT NULL GROUP BY zona ORDER BY pm25_promedio DESC;

\echo '=== 5. Optimización: índice, poda de particiones y BRIN ==='
EXPLAIN (ANALYZE, COSTS OFF) SELECT * FROM medicion_horaria WHERE id_sensor = 'T-003' ORDER BY timestamp DESC LIMIT 20;
EXPLAIN (ANALYZE, COSTS OFF) SELECT COUNT(*) FROM medicion_horaria WHERE timestamp >= '2024-03-01' AND timestamp < '2024-04-01';
SELECT experimento, variante, ROUND((segundos * 1000)::numeric, 3) AS ms FROM benchmark_resultado
WHERE tecnologia = 'PostgreSQL' ORDER BY experimento, ms;
