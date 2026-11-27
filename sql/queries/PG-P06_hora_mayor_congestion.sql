-- id: PG-P06
-- titulo: ¿Cuál es la hora de mayor congestión? (tráfico simulado)
-- tecnicas: Consulta sobre fechas: hora local, día laborable desde calendario; RANK()
-- parametros: desde=2024-01-01; hasta=2024-12-31
WITH por_hora AS (
    SELECT EXTRACT(HOUR FROM m.timestamp AT TIME ZONE 'Europe/London')::int AS hora_local,
           AVG(m.valor) AS vehiculos_hora
    FROM medicion_horaria m
    JOIN calendario c ON c.fecha = (m.timestamp AT TIME ZONE 'Europe/London')::date
    WHERE m.variable = 'trafico' AND NOT c.es_fin_de_semana AND NOT c.es_feriado
      AND m.timestamp >= %(desde)s::date AND m.timestamp < %(hasta)s::date + 1
    GROUP BY 1
)
SELECT RANK() OVER (ORDER BY vehiculos_hora DESC) AS ranking, hora_local, ROUND(vehiculos_hora) AS vehiculos_hora
FROM por_hora
ORDER BY ranking
LIMIT 5;
