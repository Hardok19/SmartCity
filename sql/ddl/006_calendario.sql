-- 006_calendario.sql — Dimensión calendario (A1.5) para consultas sobre fechas: día de la semana,
-- fin de semana y feriados oficiales de Inglaterra (bank holidays 2023–2024). Re-ejecutable.
-- Tabla de referencia de PostgreSQL, sin equivalente en MongoDB.

CREATE TABLE IF NOT EXISTS calendario (
    fecha            DATE PRIMARY KEY,
    anio             SMALLINT NOT NULL,
    mes              SMALLINT NOT NULL CHECK (mes BETWEEN 1 AND 12),
    dia              SMALLINT NOT NULL CHECK (dia BETWEEN 1 AND 31),
    dia_semana       SMALLINT NOT NULL CHECK (dia_semana BETWEEN 1 AND 7),   -- ISO: 1 = lunes
    nombre_dia       VARCHAR(10) NOT NULL,
    es_fin_de_semana BOOLEAN NOT NULL,
    es_feriado       BOOLEAN NOT NULL DEFAULT false,
    nombre_feriado   VARCHAR(80)
);
COMMENT ON TABLE calendario IS 'Dimensión de fechas 2023–2024 con feriados de Inglaterra (Londres)';

INSERT INTO calendario (fecha, anio, mes, dia, dia_semana, nombre_dia, es_fin_de_semana)
SELECT d::date, EXTRACT(YEAR FROM d), EXTRACT(MONTH FROM d), EXTRACT(DAY FROM d), EXTRACT(ISODOW FROM d),
       (ARRAY['lunes','martes','miércoles','jueves','viernes','sábado','domingo'])[EXTRACT(ISODOW FROM d)],
       EXTRACT(ISODOW FROM d) IN (6, 7)
FROM generate_series('2023-01-01'::date, '2024-12-31'::date, INTERVAL '1 day') AS d
ON CONFLICT (fecha) DO NOTHING;

UPDATE calendario c SET es_feriado = true, nombre_feriado = f.nombre
FROM (VALUES
    ('2023-01-02'::date, 'Año Nuevo (trasladado)'), ('2023-04-07', 'Viernes Santo'), ('2023-04-10', 'Lunes de Pascua'),
    ('2023-05-01', 'Feriado de mayo'), ('2023-05-08', 'Coronación de Carlos III'), ('2023-05-29', 'Feriado de primavera'),
    ('2023-08-28', 'Feriado de verano'), ('2023-12-25', 'Navidad'), ('2023-12-26', 'Boxing Day'),
    ('2024-01-01', 'Año Nuevo'), ('2024-03-29', 'Viernes Santo'), ('2024-04-01', 'Lunes de Pascua'),
    ('2024-05-06', 'Feriado de mayo'), ('2024-05-27', 'Feriado de primavera'), ('2024-08-26', 'Feriado de verano'),
    ('2024-12-25', 'Navidad'), ('2024-12-26', 'Boxing Day')
) AS f(fecha, nombre)
WHERE c.fecha = f.fecha;

CREATE INDEX IF NOT EXISTS idx_calendario_anio_mes ON calendario(anio, mes);
