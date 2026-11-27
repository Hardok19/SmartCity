-- id: PG-Q03
-- titulo: Cantidad de sensores activos e inactivos
-- tecnicas: COUNT … FILTER, COUNT(DISTINCT), GROUP BY ROLLUP
-- parametros: 
SELECT COALESCE(u.nombre_zona, 'TOTAL')                   AS zona,
       COUNT(*) FILTER (WHERE s.estado = 'activo')        AS activos,
       COUNT(*) FILTER (WHERE s.estado = 'inactivo')      AS inactivos,
       COUNT(*) FILTER (WHERE NOT s.envia_datos)          AS nunca_enviaron,
       COUNT(DISTINCT s.tipo_sensor)                      AS tipos_distintos
FROM sensores s
JOIN ubicaciones u USING (id_ubicacion)
GROUP BY ROLLUP (u.nombre_zona)
ORDER BY GROUPING(u.nombre_zona), zona;
