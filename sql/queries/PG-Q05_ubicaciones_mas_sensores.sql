-- id: PG-Q05
-- titulo: Ubicaciones con mayor cantidad de sensores
-- tecnicas: JOIN, GROUP BY, HAVING con subconsulta
-- parametros: 
SELECT u.nombre_zona AS zona, u.distrito, COUNT(s.id_sensor) AS sensores
FROM ubicaciones u
JOIN sensores s USING (id_ubicacion)
GROUP BY u.id_ubicacion, u.nombre_zona, u.distrito
HAVING COUNT(s.id_sensor) >= (                       -- zonas con al menos el promedio de sensores por zona
    SELECT AVG(n) FROM (SELECT COUNT(*) AS n FROM sensores GROUP BY id_ubicacion) AS por_zona
)
ORDER BY sensores DESC, zona;
