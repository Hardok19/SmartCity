-- Se ejecuta automáticamente la primera vez que se crea el contenedor de Postgres.
-- Ajusten/completen según su modelo E-R final (3FN, llaves foráneas, índices, etc.)

CREATE TABLE IF NOT EXISTS ubicaciones (
    id_ubicacion   SERIAL PRIMARY KEY,
    nombre_zona    VARCHAR(100) NOT NULL UNIQUE,
    distrito       VARCHAR(100),
    latitud        DECIMAL(9,6),
    longitud       DECIMAL(9,6)
);

CREATE TABLE IF NOT EXISTS categorias_sensor (
    id_categoria     SERIAL PRIMARY KEY,
    nombre_categoria VARCHAR(100) NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS organismos (
    id_organismo   SERIAL PRIMARY KEY,
    nombre         VARCHAR(150) NOT NULL,
    responsable    VARCHAR(150)
);

CREATE TABLE IF NOT EXISTS sensores (
    id_sensor        VARCHAR(20) PRIMARY KEY,     -- ej. 'S-001', mismo formato que en Mongo/HDFS
    nombre           VARCHAR(150),
    tipo_sensor      VARCHAR(50) NOT NULL,
    fecha_instalacion DATE,
    estado           VARCHAR(20) NOT NULL DEFAULT 'activo' CHECK (estado IN ('activo','inactivo')),
    id_ubicacion     INTEGER REFERENCES ubicaciones(id_ubicacion),
    id_categoria     INTEGER REFERENCES categorias_sensor(id_categoria),
    id_organismo     INTEGER REFERENCES organismos(id_organismo)
);

CREATE TABLE IF NOT EXISTS usuarios (
    id_usuario   SERIAL PRIMARY KEY,
    nombre       VARCHAR(150),
    correo       VARCHAR(150) UNIQUE
);

CREATE INDEX IF NOT EXISTS idx_sensores_tipo ON sensores(tipo_sensor);
CREATE INDEX IF NOT EXISTS idx_sensores_ubicacion ON sensores(id_ubicacion);
CREATE INDEX IF NOT EXISTS idx_sensores_estado ON sensores(estado);
