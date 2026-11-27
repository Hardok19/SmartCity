-- 002_extensiones.sql — Catálogo de variables, atributos operativos de sensores y tabla de hechos
-- medicion_horaria particionada por MES (A1.3). Re-ejecutable y sin pérdida de datos.

CREATE TABLE IF NOT EXISTS variables_medicion (
    codigo VARCHAR(40) PRIMARY KEY,
    unidad VARCHAR(30) NOT NULL
);
INSERT INTO variables_medicion VALUES
    ('pm25','ug/m3'), ('pm10','ug/m3'), ('no2','ug/m3'), ('o3','ug/m3'), ('co','ug/m3'), ('co2','ppm'),
    ('temperatura','C'), ('humedad','%'), ('precipitacion','mm'), ('viento','km/h'),
    ('trafico','vehiculos/hora'), ('energia','kWh')
ON CONFLICT (codigo) DO NOTHING;

-- Atributos operativos del catálogo (data/maestros/sensores.csv)
ALTER TABLE sensores ADD COLUMN IF NOT EXISTS origen VARCHAR(30) DEFAULT 'sintetico';
ALTER TABLE sensores ADD COLUMN IF NOT EXISTS envia_datos BOOLEAN DEFAULT true;
ALTER TABLE sensores ADD COLUMN IF NOT EXISTS fecha_baja DATE;
ALTER TABLE sensores ADD COLUMN IF NOT EXISTS id_estacion_openaq VARCHAR(30);
ALTER TABLE sensores ADD COLUMN IF NOT EXISTS frecuencia_min INTEGER DEFAULT 60;
-- Posición propia del sensor (atributo del sensor, no de la zona); si falta se usa el centroide de la zona.
ALTER TABLE sensores ADD COLUMN IF NOT EXISTS latitud DECIMAL(9,6);
ALTER TABLE sensores ADD COLUMN IF NOT EXISTS longitud DECIMAL(9,6);

-- Tabla de hechos: promedio horario por sensor y variable (la carga Spark, B3.4).
CREATE TABLE IF NOT EXISTS medicion_horaria (
    id_sensor VARCHAR(20) NOT NULL REFERENCES sensores(id_sensor) ON DELETE RESTRICT,
    timestamp TIMESTAMPTZ NOT NULL,
    variable  VARCHAR(40) NOT NULL REFERENCES variables_medicion(codigo) ON DELETE RESTRICT,
    valor     DOUBLE PRECISION NOT NULL,
    origen    VARCHAR(30) NOT NULL DEFAULT 'sintetico',
    PRIMARY KEY (id_sensor, timestamp, variable)
) PARTITION BY RANGE (timestamp);

CREATE TABLE IF NOT EXISTS medicion_horaria_default PARTITION OF medicion_horaria DEFAULT;

-- Crea una partición por mes en [p_desde, p_hasta). Omite meses ya cubiertos (p. ej. por particiones
-- anuales de versiones anteriores) y meses con filas ya guardadas en la partición DEFAULT.
CREATE OR REPLACE FUNCTION fn_crear_particiones(p_desde DATE, p_hasta DATE)
RETURNS INTEGER LANGUAGE plpgsql AS $$
DECLARE
    v_mes     DATE := date_trunc('month', p_desde)::date;
    v_nombre  TEXT;
    v_creadas INTEGER := 0;
BEGIN
    WHILE v_mes < p_hasta LOOP
        v_nombre := format('medicion_horaria_%s', to_char(v_mes, 'YYYY_MM'));
        IF to_regclass(v_nombre) IS NULL THEN
            BEGIN
                EXECUTE format('CREATE TABLE %I PARTITION OF medicion_horaria FOR VALUES FROM (%L) TO (%L)',
                               v_nombre, v_mes, (v_mes + INTERVAL '1 month')::date);
                v_creadas := v_creadas + 1;
            EXCEPTION
                WHEN invalid_object_definition OR check_violation THEN
                    RAISE NOTICE 'Mes % no creado (rango ya cubierto o con filas en DEFAULT): %', v_mes, SQLERRM;
            END;
        END IF;
        v_mes := (v_mes + INTERVAL '1 month')::date;
    END LOOP;
    RETURN v_creadas;
END $$;
COMMENT ON FUNCTION fn_crear_particiones(DATE, DATE) IS
    'Crea particiones mensuales de medicion_horaria en [p_desde, p_hasta); devuelve cuántas creó';

-- Migración: versiones anteriores creaban particiones ANUALES (medicion_horaria_2023/2024).
-- Si están vacías se reemplazan por mensuales; si tienen datos se conservan.
DO $$
DECLARE
    r RECORD;
    n BIGINT;
BEGIN
    FOR r IN
        SELECT c.relname
        FROM pg_inherits i
        JOIN pg_class c ON c.oid = i.inhrelid
        JOIN pg_class p ON p.oid = i.inhparent
        WHERE p.relname = 'medicion_horaria' AND c.relname ~ '^medicion_horaria_[0-9]{4}$'
    LOOP
        EXECUTE format('SELECT count(*) FROM %I', r.relname) INTO n;
        IF n = 0 THEN
            EXECUTE format('DROP TABLE %I', r.relname);
            RAISE NOTICE 'Partición anual vacía % reemplazada por particiones mensuales', r.relname;
        END IF;
    END LOOP;
END $$;

SELECT fn_crear_particiones('2023-01-01', '2025-01-01') AS particiones_creadas;

-- Destinos de los resultados de Spark (A2.4); contrato validado por la pareja B (spark/jobs/carga_pg.py).
CREATE TABLE IF NOT EXISTS agg_ambiente_hora_zona (
    zona VARCHAR(100) NOT NULL REFERENCES ubicaciones(nombre_zona) ON DELETE RESTRICT,
    timestamp TIMESTAMPTZ NOT NULL,
    pm25_promedio DOUBLE PRECISION, temperatura_promedio DOUBLE PRECISION,
    humedad_promedio DOUBLE PRECISION, total_mediciones BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (zona, timestamp)
);
CREATE TABLE IF NOT EXISTS spark_kpi_zona (
    zona VARCHAR(100) NOT NULL REFERENCES ubicaciones(nombre_zona) ON DELETE RESTRICT,
    fecha DATE NOT NULL,
    metrica VARCHAR(50) NOT NULL, valor DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (zona, fecha, metrica)
);
CREATE TABLE IF NOT EXISTS spark_anomalia (
    id_sensor VARCHAR(20) NOT NULL REFERENCES sensores(id_sensor) ON DELETE RESTRICT,
    timestamp TIMESTAMPTZ NOT NULL,
    variable VARCHAR(40) NOT NULL, valor DOUBLE PRECISION NOT NULL, tipo_anomalia VARCHAR(80) NOT NULL,
    PRIMARY KEY (id_sensor, timestamp, variable)
);
