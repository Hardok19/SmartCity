-- id: PG-P02
-- titulo: ¿Qué sensores han dejado de enviar información?
-- tecnicas: Función fn_sensores_sin_actividad() (nunca enviaron vs. dejaron de enviar)
-- parametros: desde=2024-12-01
SELECT *
FROM fn_sensores_sin_actividad(%(desde)s::timestamptz);
