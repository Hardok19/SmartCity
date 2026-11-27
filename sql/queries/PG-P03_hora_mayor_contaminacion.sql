-- id: PG-P03
-- titulo: Hora del día con mayor contaminación por zona
-- tecnicas: CTE, EXTRACT(HOUR) en hora local, ROW_NUMBER() OVER (PARTITION BY)
-- parametros: desde=2024-01-01; hasta=2024-12-31
WITH por_hora AS (
    SELECT u.nombre_zona AS zona,
           EXTRACT(HOUR FROM m.timestamp AT TIME ZONE 'Europe/London')::int AS hora_local,
           AVG(m.valor) AS pm25
    FROM medicion_horaria m
    JOIN sensores s    USING (id_sensor)
    JOIN ubicaciones u USING (id_ubicacion)
    WHERE m.variable = 'pm25'
      AND m.timestamp >= %(desde)s::date AND m.timestamp < %(hasta)s::date + 1
    GROUP BY 1, 2
), ordenado AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY zona ORDER BY pm25 DESC) AS puesto
    FROM por_hora
)
SELECT zona, puesto, hora_local, ROUND(pm25::numeric, 2) AS pm25_promedio
FROM ordenado
WHERE puesto <= 3
ORDER BY zona, puesto;
