-- id: PG-P05
-- titulo: ¿Cuál zona registra mayor cantidad de vehículos? (tráfico simulado)
-- tecnicas: JOIN con calendario, SUM … FILTER (laborable / fin de semana / feriado), RANK()
-- parametros: desde=2024-01-01; hasta=2024-12-31
SELECT RANK() OVER (ORDER BY SUM(m.valor) DESC)                         AS ranking,
       u.nombre_zona                                                    AS zona,
       ROUND(SUM(m.valor))                                              AS vehiculos,
       ROUND(AVG(m.valor) FILTER (WHERE NOT c.es_fin_de_semana AND NOT c.es_feriado)) AS prom_hora_laborable,
       ROUND(AVG(m.valor) FILTER (WHERE c.es_fin_de_semana OR c.es_feriado))         AS prom_hora_fin_semana_feriado
FROM medicion_horaria m
JOIN sensores s    USING (id_sensor)
JOIN ubicaciones u USING (id_ubicacion)
JOIN calendario c  ON c.fecha = (m.timestamp AT TIME ZONE 'Europe/London')::date
WHERE m.variable = 'trafico'
  AND m.timestamp >= %(desde)s::date AND m.timestamp < %(hasta)s::date + 1
GROUP BY u.nombre_zona
ORDER BY ranking;
