-- id: PG-P01
-- titulo: ¿Cuál es la temperatura promedio por zona?
-- tecnicas: JOIN, WHERE por fecha (poda de particiones), AVG/MIN/MAX
-- parametros: desde=2024-01-01; hasta=2024-12-31
SELECT u.nombre_zona               AS zona,
       ROUND(AVG(m.valor)::numeric, 2) AS temperatura_promedio,
       ROUND(MIN(m.valor)::numeric, 1) AS minima,
       ROUND(MAX(m.valor)::numeric, 1) AS maxima,
       COUNT(*)                     AS horas
FROM medicion_horaria m
JOIN sensores s    USING (id_sensor)
JOIN ubicaciones u USING (id_ubicacion)
WHERE m.variable = 'temperatura'
  AND m.timestamp >= %(desde)s::date AND m.timestamp < %(hasta)s::date + 1
GROUP BY u.nombre_zona
ORDER BY temperatura_promedio DESC;
