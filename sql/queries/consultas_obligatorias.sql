-- PG-Q01 Sensores por tipo
SELECT tipo_sensor,COUNT(*) AS cantidad FROM sensores GROUP BY tipo_sensor ORDER BY cantidad DESC;
-- PG-Q02 Sensores por ubicación
SELECT u.nombre_zona,s.id_sensor,s.nombre FROM sensores s INNER JOIN ubicaciones u USING(id_ubicacion) ORDER BY u.nombre_zona,s.id_sensor;
-- PG-Q03 Activos vs inactivos
SELECT estado, COUNT(*) AS cantidad FROM sensores GROUP BY estado ORDER BY estado;
-- PG-Q04 Sensores por organismo (incluye organismos sin sensor)
SELECT o.nombre,COUNT(s.id_sensor) AS cantidad FROM organismos o LEFT JOIN sensores s USING(id_organismo) GROUP BY o.id_organismo,o.nombre ORDER BY cantidad DESC;
-- PG-Q05 Ubicaciones con más sensores
SELECT u.nombre_zona,COUNT(s.id_sensor) AS cantidad FROM ubicaciones u LEFT JOIN sensores s USING(id_ubicacion) GROUP BY u.id_ubicacion,u.nombre_zona HAVING COUNT(s.id_sensor)>0 ORDER BY cantidad DESC;
-- PG-Q06 Instalados por año
SELECT EXTRACT(YEAR FROM fecha_instalacion)::int AS anio,COUNT(*) AS cantidad FROM sensores WHERE fecha_instalacion IS NOT NULL GROUP BY 1 ORDER BY 1;
-- PG-Q07 Sensores que nunca registraron actividad
SELECT s.id_sensor,s.nombre FROM sensores s WHERE NOT EXISTS (SELECT 1 FROM medicion_horaria m WHERE m.id_sensor=s.id_sensor) ORDER BY s.id_sensor;
-- PG-Q08 Ranking con CTE + función ventana
WITH conteos AS (SELECT u.nombre_zona AS zona,COUNT(s.id_sensor) AS total FROM ubicaciones u LEFT JOIN sensores s USING(id_ubicacion) GROUP BY u.id_ubicacion,u.nombre_zona)
SELECT zona,total,RANK() OVER(ORDER BY total DESC) AS posicion FROM conteos ORDER BY posicion,zona;
-- PG-Q09 Consulta sobre vista
SELECT zona,estado,COUNT(*) AS cantidad FROM v_sensor_detalle GROUP BY zona,estado ORDER BY zona,estado;
-- PG-Q10 Función SQL
SELECT * FROM fn_sensores_por_organismo();
-- PG-extra: media de PM2.5 por zona y día
SELECT u.nombre_zona,(m.timestamp AT TIME ZONE 'UTC')::date AS fecha,ROUND(AVG(m.valor)::numeric,2) AS pm25_promedio
FROM medicion_horaria m JOIN sensores s USING(id_sensor) JOIN ubicaciones u USING(id_ubicacion)
WHERE m.variable='pm25' GROUP BY u.nombre_zona,(m.timestamp AT TIME ZONE 'UTC')::date ORDER BY fecha,u.nombre_zona LIMIT 20;
