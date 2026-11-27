-- id: PG-Q10
-- titulo: Consulta utilizando funciones
-- tecnicas: Función SQL fn_kpi_ambiente() sobre la vista materializada mv_kpi_diario_zona
-- parametros: desde=2024-01-01; hasta=2024-12-31; zona=
SELECT *
FROM fn_kpi_ambiente(%(desde)s::date, %(hasta)s::date, NULLIF(%(zona)s, ''));
