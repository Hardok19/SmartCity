-- id: PG-Q06
-- titulo: Sensores instalados por año
-- tecnicas: EXTRACT, generate_series (años sin instalaciones), LEFT JOIN, SUM OVER (acumulado)
-- parametros: 
WITH rango AS (
    SELECT MIN(EXTRACT(YEAR FROM fecha_instalacion))::int AS desde,
           MAX(EXTRACT(YEAR FROM fecha_instalacion))::int AS hasta
    FROM sensores
)
SELECT a.anio,
       COUNT(s.id_sensor)                              AS instalados,
       SUM(COUNT(s.id_sensor)) OVER (ORDER BY a.anio)  AS acumulado
FROM rango r
CROSS JOIN LATERAL generate_series(r.desde, r.hasta) AS a(anio)
LEFT JOIN sensores s ON EXTRACT(YEAR FROM s.fecha_instalacion) = a.anio
GROUP BY a.anio
ORDER BY a.anio;
