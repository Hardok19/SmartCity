-- 001_tablas.sql — A1.2. Entidades mínimas del enunciado, en 3FN. Re-ejecutable (IF NOT EXISTS).
-- Lo ejecuta scripts/pg_init.py en orden junto con el resto de sql/ddl/*.sql.
-- Las restricciones extra (CHECK, ON DELETE RESTRICT, comentarios) están en 004_restricciones.sql para que
-- también se apliquen a bases creadas con versiones anteriores de este archivo.

CREATE TABLE IF NOT EXISTS ubicaciones (
    id_ubicacion   SERIAL PRIMARY KEY,
    nombre_zona    VARCHAR(100) NOT NULL UNIQUE,     -- clave candidata: 'Zona Norte' … (contrato.ZONAS)
    distrito       VARCHAR(100) NOT NULL,
    latitud        DECIMAL(9,6) NOT NULL,            -- coordenadas del centroide de la zona
    longitud       DECIMAL(9,6) NOT NULL
);

CREATE TABLE IF NOT EXISTS categorias_sensor (
    id_categoria     SERIAL PRIMARY KEY,
    nombre_categoria VARCHAR(100) NOT NULL UNIQUE
);

-- tipo_sensor determina la categoría (calidad_aire -> Calidad del aire). Si la categoría se guardara en
-- sensores habría una dependencia transitiva id_sensor -> tipo_sensor -> id_categoria (viola 3FN):
-- por eso el tipo es una entidad propia y sensores solo referencia el tipo.
CREATE TABLE IF NOT EXISTS tipos_sensor (
    tipo_sensor   VARCHAR(50) PRIMARY KEY,           -- mismo código que contrato.TIPOS_METRICA / Mongo `tipo`
    descripcion   VARCHAR(150) NOT NULL,
    id_categoria  INTEGER NOT NULL REFERENCES categorias_sensor(id_categoria) ON DELETE RESTRICT
);

INSERT INTO categorias_sensor (nombre_categoria)
VALUES ('Calidad del aire'), ('Clima'), ('Movilidad'), ('Energía')
ON CONFLICT (nombre_categoria) DO NOTHING;

INSERT INTO tipos_sensor (tipo_sensor, descripcion, id_categoria)
SELECT t.tipo, t.descripcion, c.id_categoria
FROM (VALUES
        ('calidad_aire', 'Estación o sensor de contaminantes (PM2.5, PM10, NO2, O3, CO)', 'Calidad del aire'),
        ('clima',        'Estación meteorológica (temperatura, humedad, precipitación, viento)', 'Clima'),
        ('trafico',      'Contador vehicular (vehículos/hora, velocidad, congestión)', 'Movilidad'),
        ('energia',      'Medidor agregado de consumo eléctrico (kWh)', 'Energía')
     ) AS t(tipo, descripcion, categoria)
JOIN categorias_sensor c ON c.nombre_categoria = t.categoria
ON CONFLICT (tipo_sensor) DO NOTHING;

CREATE TABLE IF NOT EXISTS organismos (
    id_organismo   SERIAL PRIMARY KEY,
    nombre         VARCHAR(150) NOT NULL UNIQUE,
    responsable    VARCHAR(150)
);

CREATE TABLE IF NOT EXISTS sensores (
    id_sensor         VARCHAR(20) PRIMARY KEY,       -- 'S-001', mismo identificador en Mongo, HDFS y Spark
    nombre            VARCHAR(150) NOT NULL,
    tipo_sensor       VARCHAR(50) NOT NULL REFERENCES tipos_sensor(tipo_sensor) ON DELETE RESTRICT,
    fecha_instalacion DATE,
    estado            VARCHAR(20) NOT NULL DEFAULT 'activo' CHECK (estado IN ('activo','inactivo')),
    id_ubicacion      INTEGER NOT NULL REFERENCES ubicaciones(id_ubicacion) ON DELETE RESTRICT,
    id_organismo      INTEGER REFERENCES organismos(id_organismo) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS usuarios (
    id_usuario   SERIAL PRIMARY KEY,
    nombre       VARCHAR(150) NOT NULL,
    correo       VARCHAR(150) NOT NULL UNIQUE,
    id_organismo INTEGER REFERENCES organismos(id_organismo) ON DELETE RESTRICT   -- organismo al que pertenece
);

-- Índices de este archivo: ver justificación en 005_indices.sql
CREATE INDEX IF NOT EXISTS idx_sensores_tipo ON sensores(tipo_sensor);
CREATE INDEX IF NOT EXISTS idx_sensores_ubicacion ON sensores(id_ubicacion);
CREATE INDEX IF NOT EXISTS idx_sensores_estado ON sensores(estado);
