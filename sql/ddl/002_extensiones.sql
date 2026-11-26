-- Extensiones PostgreSQL SmartCity. Re-ejecutable y sin DROP.
CREATE TABLE IF NOT EXISTS variables_medicion (
 codigo VARCHAR(40) PRIMARY KEY, unidad VARCHAR(30) NOT NULL
);
INSERT INTO variables_medicion VALUES
('pm25','ug/m3'),('pm10','ug/m3'),('no2','ug/m3'),('o3','ug/m3'),('co','ug/m3'),
('temperatura','C'),('humedad','%'),('precipitacion','mm'),('viento','km/h')
ON CONFLICT (codigo) DO NOTHING;
ALTER TABLE sensores ADD COLUMN IF NOT EXISTS origen VARCHAR(30) DEFAULT 'sintetico';
ALTER TABLE sensores ADD COLUMN IF NOT EXISTS envia_datos BOOLEAN DEFAULT true;
ALTER TABLE sensores ADD COLUMN IF NOT EXISTS fecha_baja DATE;
ALTER TABLE sensores ADD COLUMN IF NOT EXISTS id_estacion_openaq VARCHAR(30);
ALTER TABLE sensores ADD COLUMN IF NOT EXISTS frecuencia_min INTEGER DEFAULT 60;
CREATE TABLE IF NOT EXISTS medicion_horaria (
 id_sensor VARCHAR(20) NOT NULL REFERENCES sensores(id_sensor),
 timestamp TIMESTAMPTZ NOT NULL, variable VARCHAR(40) NOT NULL REFERENCES variables_medicion(codigo),
 valor DOUBLE PRECISION NOT NULL, origen VARCHAR(30) NOT NULL DEFAULT 'sintetico',
 PRIMARY KEY (id_sensor,timestamp,variable)
) PARTITION BY RANGE (timestamp);
CREATE TABLE IF NOT EXISTS medicion_horaria_2023 PARTITION OF medicion_horaria FOR VALUES FROM ('2023-01-01') TO ('2024-01-01');
CREATE TABLE IF NOT EXISTS medicion_horaria_2024 PARTITION OF medicion_horaria FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
CREATE TABLE IF NOT EXISTS medicion_horaria_default PARTITION OF medicion_horaria DEFAULT;
-- Destinos para los resultados de Spark; la pareja B debe validar este contrato antes de escribir.
CREATE TABLE IF NOT EXISTS agg_ambiente_hora_zona (
 zona VARCHAR(100) NOT NULL REFERENCES ubicaciones(nombre_zona), timestamp TIMESTAMPTZ NOT NULL,
 pm25_promedio DOUBLE PRECISION, temperatura_promedio DOUBLE PRECISION,
 humedad_promedio DOUBLE PRECISION, total_mediciones BIGINT NOT NULL DEFAULT 0,
 PRIMARY KEY(zona,timestamp)
);
CREATE TABLE IF NOT EXISTS spark_kpi_zona (
 zona VARCHAR(100) NOT NULL REFERENCES ubicaciones(nombre_zona), fecha DATE NOT NULL,
 metrica VARCHAR(50) NOT NULL, valor DOUBLE PRECISION NOT NULL,
 PRIMARY KEY(zona,fecha,metrica)
);
CREATE TABLE IF NOT EXISTS spark_anomalia (
 id_sensor VARCHAR(20) NOT NULL REFERENCES sensores(id_sensor), timestamp TIMESTAMPTZ NOT NULL,
 variable VARCHAR(40) NOT NULL, valor DOUBLE PRECISION NOT NULL, tipo_anomalia VARCHAR(80) NOT NULL,
 PRIMARY KEY(id_sensor,timestamp,variable)
);
