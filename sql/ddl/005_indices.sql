-- 005_indices.sql — A1.4. Índices justificados. Re-ejecutable (IF NOT EXISTS).
-- Regla general: PostgreSQL NO crea índices para las FK; sin ellos cada JOIN o cada verificación de
-- ON DELETE RESTRICT recorre la tabla hija completa.

-- ---------------------------------------------------------------- catálogo
-- justifica: FK sensores.tipo_sensor; PG-Q01 agrupa por tipo y v_sensor_detalle une con tipos_sensor.
CREATE INDEX IF NOT EXISTS idx_sensores_tipo ON sensores(tipo_sensor);
-- justifica: FK sensores.id_ubicacion; PG-Q02/Q05/Q08 unen y agrupan sensores por zona.
CREATE INDEX IF NOT EXISTS idx_sensores_ubicacion ON sensores(id_ubicacion);
-- justifica: FK sensores.id_organismo; PG-Q04 y fn_sensores_por_organismo() (LEFT JOIN desde organismos).
CREATE INDEX IF NOT EXISTS idx_sensores_organismo ON sensores(id_organismo);
-- justifica: PG-Q06 agrupa por año de instalación y filtra rangos de fechas de instalación.
CREATE INDEX IF NOT EXISTS idx_sensores_fecha ON sensores(fecha_instalacion);
-- justifica: PG-Q03 cuenta por estado.
CREATE INDEX IF NOT EXISTS idx_sensores_estado ON sensores(estado);
-- justifica: índice PARCIAL: el dashboard consulta casi siempre solo sensores activos por tipo;
--            indexar solo los activos lo hace más pequeño que un índice completo.
CREATE INDEX IF NOT EXISTS idx_sensores_activos_tipo ON sensores(tipo_sensor) WHERE estado = 'activo';
-- justifica: FK tipos_sensor.id_categoria (borrar/consultar una categoría no recorre tipos_sensor).
CREATE INDEX IF NOT EXISTS idx_tipos_sensor_categoria ON tipos_sensor(id_categoria);
-- justifica: FK usuarios.id_organismo (usuarios de un organismo).
CREATE INDEX IF NOT EXISTS idx_usuarios_organismo ON usuarios(id_organismo);

-- ---------------------------------------------------------------- hechos (medicion_horaria, particionada)
-- Los índices sobre la tabla particionada se crean en cada partición mensual automáticamente.
-- justifica: consulta típica "últimas lecturas de un sensor" (lecturas_pg, PG-P02): búsqueda por sensor
--            y orden por fecha descendente sin paso de ordenamiento.
CREATE INDEX IF NOT EXISTS idx_medicion_sensor_fecha ON medicion_horaria(id_sensor, timestamp DESC);
-- justifica: filtros por variable + rango de fechas (PG-P01, PG-P03, PG-P04, mv_kpi_diario_zona).
CREATE INDEX IF NOT EXISTS idx_medicion_variable_fecha ON medicion_horaria(variable, timestamp DESC);
-- justifica: BRIN en timestamp: los datos se cargan en orden temporal, así que cada bloque de páginas
--            cubre un rango de fechas estrecho. El índice ocupa unos pocos KB (frente a MB de un B-tree)
--            y sirve para rangos de fechas amplios. Comparación en docs/resultados/postgres_optimizacion.md.
CREATE INDEX IF NOT EXISTS idx_medicion_fecha_brin ON medicion_horaria USING BRIN(timestamp);

-- ---------------------------------------------------------------- resultados de Spark
-- justifica: agg_ambiente_hora_zona y spark_kpi_zona ya tienen PK (zona, timestamp/fecha…): el índice
--            de la PK sirve para filtrar por zona y rango de fechas. spark_anomalia: búsqueda por tipo.
CREATE INDEX IF NOT EXISTS idx_spark_anomalia_tipo ON spark_anomalia(tipo_anomalia, timestamp DESC);

COMMENT ON INDEX idx_sensores_activos_tipo IS 'Parcial: solo sensores activos (consultas del dashboard)';
COMMENT ON INDEX idx_medicion_sensor_fecha IS 'Últimas lecturas por sensor sin ordenar en memoria';
COMMENT ON INDEX idx_medicion_fecha_brin IS 'BRIN: datos insertados en orden temporal; índice diminuto para rangos amplios';
