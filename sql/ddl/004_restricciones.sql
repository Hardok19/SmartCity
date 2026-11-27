-- 004_restricciones.sql — A1.2. Restricciones de integridad, migración a 3FN y documentación del esquema.
-- Re-ejecutable: cada restricción se agrega solo si no existe. Deja una base creada con versiones
-- anteriores de 001/002 en el mismo estado que una base nueva.

-- Ayuda: agrega una restricción si no existe (función temporal de la sesión).
CREATE OR REPLACE FUNCTION pg_temp.agregar_restriccion(p_tabla TEXT, p_nombre TEXT, p_definicion TEXT)
RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = p_nombre) THEN
        EXECUTE format('ALTER TABLE %I ADD CONSTRAINT %I %s', p_tabla, p_nombre, p_definicion);
    END IF;
END $$;

-- ---------------------------------------------------------------- migración a 3FN
-- Versiones anteriores guardaban sensores.id_categoria (dependencia transitiva vía tipo_sensor).
ALTER TABLE sensores DROP COLUMN IF EXISTS id_categoria;
SELECT pg_temp.agregar_restriccion('sensores', 'sensores_tipo_sensor_fkey',
    'FOREIGN KEY (tipo_sensor) REFERENCES tipos_sensor(tipo_sensor) ON DELETE RESTRICT');
ALTER TABLE usuarios ADD COLUMN IF NOT EXISTS id_organismo INTEGER;
SELECT pg_temp.agregar_restriccion('usuarios', 'usuarios_id_organismo_fkey',
    'FOREIGN KEY (id_organismo) REFERENCES organismos(id_organismo) ON DELETE RESTRICT');
SELECT pg_temp.agregar_restriccion('organismos', 'organismos_nombre_key', 'UNIQUE (nombre)');

-- ---------------------------------------------------------------- ON DELETE RESTRICT en todas las FK
-- No se permite borrar un catálogo (zona, organismo, tipo, sensor, variable) que todavía se usa.
DO $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN
        SELECT c.conname, t.relname AS tabla, pg_get_constraintdef(c.oid) AS def
        FROM pg_constraint c
        JOIN pg_class t ON t.oid = c.conrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        WHERE c.contype = 'f' AND n.nspname = 'public' AND c.conparentid = 0 AND c.confdeltype <> 'r'
    LOOP
        EXECUTE format('ALTER TABLE %I DROP CONSTRAINT %I', r.tabla, r.conname);
        EXECUTE format('ALTER TABLE %I ADD CONSTRAINT %I %s ON DELETE RESTRICT', r.tabla, r.conname,
                       regexp_replace(r.def, ' ON DELETE \w+( \w+)?', ''));
    END LOOP;
END $$;

-- ---------------------------------------------------------------- NOT NULL
ALTER TABLE ubicaciones ALTER COLUMN distrito SET NOT NULL;
ALTER TABLE ubicaciones ALTER COLUMN latitud SET NOT NULL;
ALTER TABLE ubicaciones ALTER COLUMN longitud SET NOT NULL;
ALTER TABLE sensores ALTER COLUMN nombre SET NOT NULL;
ALTER TABLE sensores ALTER COLUMN id_ubicacion SET NOT NULL;
ALTER TABLE usuarios ALTER COLUMN nombre SET NOT NULL;
ALTER TABLE usuarios ALTER COLUMN correo SET NOT NULL;

-- ---------------------------------------------------------------- CHECK
SELECT pg_temp.agregar_restriccion('ubicaciones', 'ck_ubicaciones_latitud',  'CHECK (latitud BETWEEN -90 AND 90)');
SELECT pg_temp.agregar_restriccion('ubicaciones', 'ck_ubicaciones_longitud', 'CHECK (longitud BETWEEN -180 AND 180)');
SELECT pg_temp.agregar_restriccion('sensores', 'ck_sensores_coordenadas',
    'CHECK ((latitud IS NULL OR latitud BETWEEN -90 AND 90) AND (longitud IS NULL OR longitud BETWEEN -180 AND 180))');
SELECT pg_temp.agregar_restriccion('sensores', 'ck_sensores_fecha_instalacion', 'CHECK (fecha_instalacion <= CURRENT_DATE)');
SELECT pg_temp.agregar_restriccion('sensores', 'ck_sensores_fecha_baja',
    'CHECK (fecha_baja IS NULL OR fecha_instalacion IS NULL OR fecha_baja >= fecha_instalacion)');
SELECT pg_temp.agregar_restriccion('sensores', 'ck_sensores_baja_inactivo',
    'CHECK (fecha_baja IS NULL OR estado = ''inactivo'')');
SELECT pg_temp.agregar_restriccion('sensores', 'ck_sensores_frecuencia', 'CHECK (frecuencia_min > 0)');
SELECT pg_temp.agregar_restriccion('sensores', 'ck_sensores_origen',
    'CHECK (origen IN (''openaq'', ''open-meteo'', ''sintetico''))');
SELECT pg_temp.agregar_restriccion('usuarios', 'ck_usuarios_correo',
    'CHECK (correo ~ ''^[^@[:space:]]+@[^@[:space:]]+\.[^@[:space:]]+$'')');
SELECT pg_temp.agregar_restriccion('medicion_horaria', 'ck_medicion_origen',
    'CHECK (origen IN (''openaq'', ''open-meteo'', ''sintetico'', ''prueba_pg''))');
SELECT pg_temp.agregar_restriccion('medicion_horaria', 'ck_medicion_valor_finito', 'CHECK (valor <> ''NaN''::float8)');
SELECT pg_temp.agregar_restriccion('medicion_horaria', 'ck_medicion_humedad',
    'CHECK (variable <> ''humedad'' OR valor BETWEEN 0 AND 100)');
SELECT pg_temp.agregar_restriccion('medicion_horaria', 'ck_medicion_no_negativo',
    'CHECK (variable = ''temperatura'' OR valor >= 0)');

-- ---------------------------------------------------------------- documentación (COMMENT ON)
COMMENT ON TABLE ubicaciones IS 'Zonas de la ciudad (Londres): una ubicación por zona del contrato, con distrito y centroide';
COMMENT ON COLUMN ubicaciones.nombre_zona IS 'Clave candidata; valores exactos de contrato.ZONAS';
COMMENT ON TABLE categorias_sensor IS 'Categorías de sensores (Calidad del aire, Clima, Movilidad, Energía)';
COMMENT ON TABLE tipos_sensor IS 'Tipo de sensor; determina su categoría (tabla propia para cumplir 3FN)';
COMMENT ON TABLE organismos IS 'Organismos responsables de los sensores';
COMMENT ON TABLE sensores IS 'Catálogo maestro de sensores; id_sensor es el mismo en MongoDB, HDFS y Spark';
COMMENT ON COLUMN sensores.estado IS 'activo | inactivo (contrato.ESTADOS_SENSOR)';
COMMENT ON COLUMN sensores.envia_datos IS 'false = sensor registrado que nunca envió datos (PG-Q07)';
COMMENT ON COLUMN sensores.fecha_baja IS 'Fecha desde la que dejó de enviar datos (solo sensores inactivos)';
COMMENT ON COLUMN sensores.origen IS 'openaq | open-meteo | sintetico';
COMMENT ON TABLE usuarios IS 'Usuarios del sistema (ficticios), asociados a un organismo';
COMMENT ON TABLE variables_medicion IS 'Variables medidas y su unidad';
COMMENT ON TABLE medicion_horaria IS 'Promedio horario por sensor y variable; particionada por mes; la carga Spark (B3.4)';
COMMENT ON COLUMN medicion_horaria.origen IS 'openaq | open-meteo | sintetico | prueba_pg (datos de prueba de scripts/pg_generar_mediciones.py)';
COMMENT ON TABLE agg_ambiente_hora_zona IS 'Agregado zona-hora calculado por Spark (B3.4)';
COMMENT ON TABLE spark_kpi_zona IS 'KPI diario por zona y métrica, formato largo, calculado por Spark (B3.4)';
COMMENT ON TABLE spark_anomalia IS 'Lecturas anómalas detectadas por Spark (marca de calidad, z-score, IQR)';
