-- SP-Q07 — ¿Qué sensores presentan valores anormales?
-- Consulta directamente la vista 'lecturas_crudas', cargada desde MongoDB por ejecutar_consultas.py.
-- NO usa el Parquet curado porque el ETL retiró documentos con calidad_dato.valido=false.
-- Detecta (a) flags del generador y (b) valores extremos con Z-score (|z| >= 3).
-- El Z-score compara la variable principal por sensor y se calcula con muestras válidas.
-- Muestra hasta 50 casos de cada método (100 en total) para la interfaz/demo.
-- NOTA: las marcas de calidad pertenecen al documento; los datos son reales y sintéticos.
WITH base_raw AS (
    SELECT
        sensor_id AS id_sensor,
        ubicacion.zona AS zona,
        timestamp,
        tipo AS categoria,
        origen,
        mediciones.pm25 AS pm25,
        mediciones.pm10 AS pm10,
        mediciones.no2 AS no2,
        mediciones.co AS co,
        mediciones.o3 AS o3,
        mediciones.temperatura AS temperatura,
        mediciones.humedad AS humedad,
        COALESCE(calidad_dato.valido, TRUE) AS calidad_valida,
        COALESCE(calidad_dato.flags, CAST(array() AS ARRAY<STRING>)) AS flags
    FROM lecturas_crudas
    WHERE sensor_id IS NOT NULL AND timestamp IS NOT NULL
),
base AS (
    SELECT
        id_sensor, zona, timestamp, categoria, origen, calidad_valida, flags,
        CASE
            WHEN categoria = 'calidad_aire' AND pm25 IS NOT NULL THEN 'pm25'
            WHEN categoria = 'calidad_aire' AND pm10 IS NOT NULL THEN 'pm10'
            WHEN categoria = 'calidad_aire' AND no2 IS NOT NULL THEN 'no2'
            WHEN categoria = 'calidad_aire' AND co IS NOT NULL THEN 'co'
            WHEN categoria = 'calidad_aire' AND o3 IS NOT NULL THEN 'o3'
            WHEN categoria = 'clima' AND array_contains(flags, 'fuera_de_rango') THEN 'humedad'
            WHEN categoria = 'clima' THEN 'temperatura'
        END AS tipo_metrica,
        CASE
            WHEN categoria = 'calidad_aire' THEN COALESCE(pm25, pm10, no2, co, o3)
            WHEN categoria = 'clima' AND array_contains(flags, 'fuera_de_rango') THEN humedad
            WHEN categoria = 'clima' THEN temperatura
        END AS valor
    FROM base_raw
),
estadisticas AS (
    SELECT
        id_sensor, tipo_metrica,
        AVG(valor) AS media,
        STDDEV_SAMP(valor) AS desviacion
    FROM base
    WHERE calidad_valida = TRUE AND valor IS NOT NULL AND NOT isnan(valor)
    GROUP BY id_sensor, tipo_metrica
),
puntuadas AS (
    SELECT
        b.id_sensor, b.zona, b.timestamp, b.tipo_metrica, b.valor,
        b.origen, b.calidad_valida, b.flags,
        CASE WHEN s.desviacion IS NULL OR s.desviacion = 0 THEN NULL
             ELSE (b.valor - s.media) / s.desviacion END AS z_score
    FROM base b
    LEFT JOIN estadisticas s
      ON b.id_sensor = s.id_sensor AND b.tipo_metrica = s.tipo_metrica
    WHERE b.valor IS NOT NULL AND NOT isnan(b.valor)
),
marcadas AS (
    SELECT
        id_sensor, zona, timestamp, tipo_metrica,
        ROUND(valor, 2) AS valor,
        'marca_calidad' AS metodo_deteccion,
        array_join(flags, ', ') AS detalle,
        ROUND(z_score, 2) AS z_score,
        origen
    FROM puntuadas
    WHERE calidad_valida = FALSE
    ORDER BY timestamp DESC
    LIMIT 50
),
extremas AS (
    SELECT
        id_sensor, zona, timestamp, tipo_metrica,
        ROUND(valor, 2) AS valor,
        'z_score' AS metodo_deteccion,
        '|z| >= 3' AS detalle,
        ROUND(z_score, 2) AS z_score,
        origen
    FROM puntuadas
    WHERE calidad_valida = TRUE AND ABS(z_score) >= 3
    ORDER BY ABS(z_score) DESC
    LIMIT 50
)
SELECT * FROM marcadas
UNION ALL
SELECT * FROM extremas
ORDER BY metodo_deteccion, timestamp DESC
