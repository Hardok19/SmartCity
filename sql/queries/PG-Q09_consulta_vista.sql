-- id: PG-Q09
-- titulo: Consulta utilizando vistas
-- tecnicas: Vista v_sensor_detalle (4 JOIN encapsulados), STRING_AGG(DISTINCT)
-- parametros: 
SELECT zona, categoria,
       COUNT(*)                                     AS sensores,
       COUNT(*) FILTER (WHERE estado = 'activo')    AS activos,
       STRING_AGG(DISTINCT organismo, ', ')         AS organismos
FROM v_sensor_detalle
GROUP BY zona, categoria
ORDER BY zona, categoria;
