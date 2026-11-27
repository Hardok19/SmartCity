-- 007_benchmark.sql — Resultados de experimentos de rendimiento (A2.5 y matriz comparativa C1).
CREATE TABLE IF NOT EXISTS benchmark_resultado (
    id            SERIAL PRIMARY KEY,
    experimento   VARCHAR(20)  NOT NULL,          -- E1…E7 o A2.5-1…
    tecnologia    VARCHAR(20)  NOT NULL CHECK (tecnologia IN ('PostgreSQL', 'MongoDB', 'HDFS', 'Spark')),
    variante      VARCHAR(80)  NOT NULL,          -- con_indice, sin_indice, brin, btree…
    escala_filas  BIGINT,
    segundos      DOUBLE PRECISION NOT NULL CHECK (segundos >= 0),
    detalle       TEXT,
    medido_en     TIMESTAMPTZ NOT NULL DEFAULT now()
);
COMMENT ON TABLE benchmark_resultado IS 'Tiempos de los experimentos (mediana de repeticiones)';
