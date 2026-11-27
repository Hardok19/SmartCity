-- id: PG-Q02
-- titulo: Sensores por ubicación
-- tecnicas: INNER JOIN, WHERE con parámetro, ORDER BY
-- parametros: zona=
SELECT u.nombre_zona AS zona, u.distrito, s.id_sensor, s.nombre, s.tipo_sensor, s.estado
FROM sensores s
INNER JOIN ubicaciones u ON u.id_ubicacion = s.id_ubicacion
WHERE (%(zona)s = '' OR u.nombre_zona = %(zona)s)      -- zona vacía = todas
ORDER BY u.nombre_zona, s.id_sensor;
