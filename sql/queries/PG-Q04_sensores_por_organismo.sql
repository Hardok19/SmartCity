-- id: PG-Q04
-- titulo: Número de sensores por organismo responsable
-- tecnicas: LEFT JOIN (incluye organismos sin sensores), GROUP BY
-- parametros: 
SELECT o.nombre                                     AS organismo,
       o.responsable,
       COUNT(s.id_sensor)                           AS sensores,
       COUNT(s.id_sensor) FILTER (WHERE s.estado = 'activo') AS activos
FROM organismos o
LEFT JOIN sensores s USING (id_organismo)
GROUP BY o.id_organismo, o.nombre, o.responsable
ORDER BY sensores DESC, organismo;
