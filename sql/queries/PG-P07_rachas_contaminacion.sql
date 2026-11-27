-- id: PG-P07
-- titulo: Rachas de días consecutivos con PM2.5 diario sobre el umbral
-- tecnicas: WITH RECURSIVE (encadena días consecutivos), NOT EXISTS, vista materializada
-- parametros: umbral=15; desde=2023-01-01; hasta=2024-12-31
-- Umbral por defecto: 15 µg/m³, guía diaria de la OMS (2021) para PM2.5.
WITH RECURSIVE dias AS (
    SELECT zona, fecha
    FROM mv_kpi_diario_zona
    WHERE pm25_promedio > %(umbral)s AND fecha BETWEEN %(desde)s::date AND %(hasta)s::date
), racha AS (
    -- caso base: días que inician una racha (el día anterior no superó el umbral)
    SELECT d.zona, d.fecha AS inicio, d.fecha AS fin, 1 AS dias
    FROM dias d
    WHERE NOT EXISTS (SELECT 1 FROM dias p WHERE p.zona = d.zona AND p.fecha = d.fecha - 1)
    UNION ALL
    -- paso recursivo: se agrega el día siguiente si también superó el umbral
    SELECT r.zona, r.inicio, d.fecha, r.dias + 1
    FROM racha r
    JOIN dias d ON d.zona = r.zona AND d.fecha = r.fin + 1
)
SELECT zona, inicio, MAX(fin) AS fin, MAX(dias) AS dias_consecutivos
FROM racha
GROUP BY zona, inicio
ORDER BY dias_consecutivos DESC, zona, inicio
LIMIT 15;
