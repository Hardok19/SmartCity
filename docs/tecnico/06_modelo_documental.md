# 6. Modelo documental (MongoDB)

## 6.1 Qué datos van a MongoDB y por qué

MongoDB guarda los datos **semiestructurados y de esquema variable** del proyecto: las lecturas de los sensores (`lectura`, ≈ 1,07 M documentos), los eventos detectados (`evento`), las alertas (`alerta`) y los incidentes reportados en texto libre (`incidente`). El catálogo estable (sensores, zonas, organismos y usuarios) vive en PostgreSQL, donde la integridad referencial sí importa.

Tres propiedades de estos datos hacen que el modelo documental encaje mejor que el relacional:

1. **Cada tipo de sensor mide cosas distintas.** Un sensor de aire reporta `{pm25, pm10, co, no2}`, uno de clima `{temperatura, humedad, precipitacion, viento}` y uno de ruido `{db_promedio, db_max}`. Todo eso vive en un solo subdocumento `mediciones` sin columnas fijas.
2. **El esquema cambia con el tiempo.** Desde el 2024-03-01, la mitad de los sensores virtuales de aire pasa a *firmware 2.x* y agrega `mediciones.o3`, `mediciones.co2` y `dispositivo.bateria_pct`. Los documentos anteriores no tienen esos campos y conviven sin migración.
3. **Hay arreglos de largo variable y texto libre.** Las alertas acumulan `acciones[]`; los incidentes, `historial[]` y `reportes_ciudadanos[]` con descripciones en español que se buscan con un índice `text`.

El diagrama completo está en [docs/diagramas/modelo_documental.md](../diagramas/modelo_documental.md).

## 6.2 Colecciones

| Colección | Documento = | Campos obligatorios (validador) | Variable por documento |
|---|---|---|---|
| `lectura` | una lectura de un sensor en un instante | `sensor_id, tipo, timestamp, ubicacion{id_zona, zona}` | `mediciones` según tipo y firmware; `dispositivo`, `calidad_dato` opcionales |
| `evento` | un episodio (contaminación alta, falla de sensor, lluvia intensa, ola de calor) | `tipo_evento, severidad, inicio, zona` | `detalles` distinto por `tipo_evento`; `fin` y `sensores` opcionales |
| `alerta` | una superación de umbral (`config/umbrales.yaml`) | `regla, variable, valor, umbral, estado, emitida, zona` | `acciones[]` crece con cada intervención |
| `incidente` | un reporte urbano en texto libre | `categoria, titulo, descripcion, reportado, zona, estado_actual` | `historial[]`, `reportes_ciudadanos[]`, `heridos` (solo accidentes), `adjuntos` (~20 %) |

**Validación.** Cada colección tiene un `$jsonSchema` **flexible**: exige solo los campos que las consultas necesitan para funcionar (identificador, tipo, fecha, zona) y deja libre el resto. Usa `validationLevel: "moderate"` (los documentos que ya existen y no cumplen no bloquean actualizaciones) y `validationAction: "error"`. Así, insertar una lectura sin `sensor_id` falla, pero una lectura con un campo nuevo se acepta.

**Nombres de campos.** `lectura.sensor_id` es el mismo identificador que `id_sensor` en PostgreSQL y HDFS (`S-001`…). `lectura.tipo` es la categoría (`calidad_aire`, `clima`…). El puente hacia las columnas estándar del contrato (`zona, timestamp, id_sensor, tipo_metrica, valor, fuente`) está en [scripts/mongo_contrato.py](../../scripts/mongo_contrato.py): para `calidad_aire`, `valor` = PM2.5.

## 6.3 Embebido frente a referencia

| Relación | Decisión | Motivo |
|---|---|---|
| lectura → ubicación (zona, tipo de zona, punto GeoJSON) | **Embebido** | Se lee siempre junto con la lectura (filtros por zona, `$geoNear`) y no cambia. Duplicarla evita un `$lookup` en cada una de 1 M lecturas. |
| lectura → mediciones, dispositivo, calidad del dato | **Embebido** | Pertenecen a la lectura (relación 1:1) y su forma varía por tipo. |
| alerta / evento / incidente → zona | **Embebido** (`{id_zona, nombre, geo}`) | Se agrupa y filtra por zona en todas las consultas (MG-Q05, MG-Q08); la zona es un catálogo de 5 valores que no cambia. |
| alerta → acciones; incidente → historial, reportes | **Embebido** (arreglos) | Se consultan siempre con su documento padre, son pocos por documento y no tienen sentido por separado. |
| alerta / evento → sensor | **Referencia** (`sensor_id`, `sensores[]`) | El sensor es un catálogo maestro de PostgreSQL; aquí solo se guarda su identificador. Copiar todo el sensor desincronizaría los datos cuando cambie su estado. |
| lectura → alerta | **Sin vínculo físico** | La alerta se deriva de una racha de lecturas (`valor`, `emitida`). Se relacionan por `sensor_id` cuando hace falta (`$lookup` de `alerta` en MG-Q09). |

Regla aplicada: se **embebe** lo que se lee junto y tiene tamaño acotado; se **referencia** lo que es un catálogo compartido o puede crecer sin límite.

## 6.4 Por qué sería incómodo en el modelo relacional

- **Columnas dispersas.** Una tabla `lectura` con una columna por variable (`pm25, pm10, co, no2, o3, co2, temperatura, humedad, precipitacion, viento, db_promedio…`) quedaría casi toda en `NULL`: una lectura de clima no usa ninguna columna de aire. La alternativa EAV (una fila por variable) multiplica las filas ×4–6 y obliga a pivotar en cada consulta.
- **Una migración por cada tipo o firmware nuevo.** El firmware 2.x habría exigido `ALTER TABLE … ADD COLUMN co2, bateria_pct` y decidir qué hacer con el histórico. Un sensor nuevo (p. ej., `flujo_peatonal`, que aparece en la demo CRUD) habría requerido otra migración.
- **Arreglos anidados.** `historial[]`, `reportes_ciudadanos[]` y `acciones[]` serían tres tablas hijas más, con JOIN en cada lectura del incidente o de la alerta.
- **Texto libre.** La búsqueda en descripciones de incidentes en español usa el índice `text` de Mongo (con raíces del español) sin configurar diccionarios.
- **JSONB en PostgreSQL** resolvería parte de esto, pero perdería la validación parcial por `$jsonSchema` y el Aggregation Framework (`$facet`, `$bucket`, `$setWindowFields`, `$geoNear`) sobre el mismo almacenamiento. Este matiz se discute en la matriz comparativa (C2).

## 6.5 Cuándo **no** usar MongoDB en este proyecto

- **Catálogos con integridad referencial** (sensor → tipo → categoría → zona → organismo): un sensor no debe poder apuntar a un organismo inexistente. Esto va en PostgreSQL, con FK y 3FN.
- **Consultas con muchos JOIN** entre entidades estables (PG-Q02, PG-Q04): `$lookup` es más lento y verboso que un JOIN con índices.
- **Agregaciones masivas sobre todo el histórico** (8 análisis, correlaciones, MLlib): las hace Spark sobre Parquet en HDFS, en paralelo; Mongo sirve consultas operativas acotadas por índice.
- **Transacciones de varios documentos** con reglas de negocio estrictas: Mongo las soporta, pero el modelo relacional es más natural.

## 6.6 Consultas y operaciones

- CRUD de demostración: [mongo/03_crud_demo.js](../../mongo/03_crud_demo.js) y `conectores_mongo_spark.crud()`, sobre la colección `demo_crud`.
- Consultas MG-Q01…Q10 y preguntas MG-P01…P05: [mongo/queries/](../../mongo/queries/) (mongosh) y `conectores_mongo_spark.CONSULTAS_MONGO` (mismos pipelines, con parámetros, ejecutables desde el dashboard).
- Experimento de índices (IXSCAN frente a COLLSCAN): [scripts/mongo_experimento_indices.py](../../scripts/mongo_experimento_indices.py) → `docs/resultados/mongo_indices.md`.
- Exportación a HDFS para Hadoop/Spark: `scripts/subir_crudos_hdfs.py --fuentes mongo` o [mongo/04_export.sh](../../mongo/04_export.sh) → `/smartcity/raw/mongo_exports/<coleccion>/fecha=…/`, con verificación de que el conteo en Mongo es igual al de HDFS.
