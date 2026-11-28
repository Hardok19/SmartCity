# Documento de atributos — SmartCity Analytics (CE-3191, II-2026)

El curso desarrolla dos atributos del graduado: **Análisis Financiero (AF)** y **Aprendizaje Continuo (AC, nivel
medio)**. Para cada uno se describe, como pide el enunciado (§11): (1) cómo se aplicó en la solución del proyecto,
(2) el impacto del proyecto en la sociedad y (3) la retroalimentación obtenida.

El documento describe lo que el equipo hizo y decidió, con evidencia del repositorio.

Como anexo, [DICCIONARIO_DATOS.md](DICCIONARIO_DATOS.md) define cada atributo de datos del proyecto (tipo, fuente y
restricciones) en PostgreSQL, MongoDB, HDFS y Spark.

---

## 1. Análisis Financiero (AF)

> *Aplica el conocimiento y la comprensión de los principios de gestión de ingeniería y la toma de decisiones
> económicas y los aplica al propio trabajo, como miembro y líder de un equipo, y para gestionar proyectos en entornos
> multidisciplinarios.*

### 1.1 Aplicación del atributo en la solución

**Decisión económica principal: infraestructura propia con software libre en lugar de servicios gestionados.** Las
cuatro tecnologías corren en contenedores Docker en las computadoras del equipo, con licencias sin costo (PostgreSQL
License; SSPL para MongoDB Community Server, de código disponible; Apache 2.0 para Hadoop y Spark). El costo de
licencias fue cero. Para dimensionar lo
que se evitó, se estimó lo que costaría el mismo entorno con servicios gestionados equivalentes en la nube
(precios de lista consultados el 2026-11-27, región us-east-1, bajo demanda; son aproximados y cambian con el
tiempo):

| Componente | Servicio gestionado equivalente | Precio de referencia | Costo mensual estimado |
|---|---|---|---|
| PostgreSQL | Amazon RDS for PostgreSQL `db.t4g.medium` (2 vCPU, 4 GB) | ≈ USD 47/mes bajo demanda ([Bytebase, 2026](https://www.bytebase.com/dbcost/rds/instance/db.t4g.medium/)) | ≈ USD 47 |
| MongoDB | MongoDB Atlas M10 (2 vCPU, 2 GB) | USD 0,08/h ≈ USD 57/mes ([MongoDB](https://www.mongodb.com/pricing)) | ≈ USD 57 |
| Hadoop + Spark | Amazon EMR, 3 nodos `m5.xlarge` (EC2 ≈ USD 0,192/h + EMR ≈ USD 0,048/h por nodo) ([AWS EMR](https://aws.amazon.com/emr/pricing/)) | ≈ USD 0,72/h por clúster | ≈ USD 127 encendido 8 h × 22 días; ≈ USD 526 si queda encendido 24/7 |
| **Total** | | | **≈ USD 230/mes** con uso de oficina, **≈ USD 630/mes** 24/7 |

Para las ~8 semanas del proyecto eso equivale a ≈ USD 460–1.260, sin contar almacenamiento, transferencia de datos ni
respaldos. La alternativa local costó el consumo eléctrico de equipos que ya teníamos, a cambio de **más horas de
administración**: el equipo resolvió problemas que un servicio gestionado habría evitado, como el puerto 5432 ocupado
por un PostgreSQL local, el NodeManager sin Python 3, el límite de memoria de los contenedores y la red entre Windows
y Spark. Ese es el intercambio que se analizó: **costo monetario frente a costo en tiempo y riesgo de operación**.

**Otras decisiones con criterio económico:**

| Decisión | Alternativa descartada | Criterio |
|---|---|---|
| Fuentes abiertas (OpenAQ con clave gratuita, Open-Meteo sin clave) | Datasets comerciales | Costo cero; licencias compatibles con uso académico |
| Datos sintéticos anclados a datos reales para llegar a los volúmenes | Comprar o descargar decenas de GB | El enunciado lo permite; cabe en el disco y la RAM disponibles |
| Límites de memoria por contenedor (`mem_limit`), 3 DataNodes y 1 worker | Comprar más RAM o un servidor | El entorno completo cabe en ~8 GB: no hubo gasto en hardware |
| Streamlit | Power BI | Streamlit es gratuito y se integra en Python con los cuatro motores; Power BI requiere licencia para compartir informes |
| Benchmarks hasta 5 M filas | Pruebas a decenas de GB | Lo que permite la máquina; se documentó la proyección a ×100 en lugar de pagar infraestructura para medirla |

**Gestión del proyecto y del equipo.** El trabajo se dividió en dos parejas con responsabilidades claras (A:
PostgreSQL + Hadoop; B: MongoDB + Spark; ambas: dashboard y documentación). Se planificó en `TODO.md` con:

- tareas numeradas, cada una con pasos, criterio de aceptación y responsable;
- **dependencias cruzadas explícitas** (por ejemplo, las tablas destino de A bloqueaban la carga de B a PostgreSQL) y
  una **ruta crítica** (F1 → F2 → A1 ∥ B1 → A3 ∥ B2 → B3 → B4 → D → E);
- un **registro de decisiones** (D-01…D-06) con su justificación, para que un cambio de alcance no se perdiera;
- un **contrato de datos** (`contrato.py`) y un módulo común (`comun.py`) acordados al inicio, que permitieron a
  cada pareja trabajar en paralelo sin bloquear a la otra;
- ramas por pareja y *pull requests* a `main`.

### 1.2 Impacto del proyecto en la sociedad

Una municipalidad que quiera monitorear la calidad del aire no necesita empezar con servicios en la nube de cientos de
dólares al mes: la misma arquitectura corre con software libre en un servidor propio, y puede migrarse después a
servicios gestionados cuando el volumen o la disponibilidad requerida lo justifiquen. Esto importa especialmente para
gobiernos locales con presupuesto limitado. Las decisiones que el sistema apoya también tienen un componente económico:
detectar zonas y horas críticas de contaminación permite focalizar medidas (restricción vehicular en hora pico,
alertas a población vulnerable) en lugar de aplicarlas a toda la ciudad, y los KPIs de energía (consumo por zona,
picos, variación respecto al período anterior) ayudan a planificar la demanda.

### 1.3 Retroalimentación obtenida

- **El costo «gratis» no es gratis.** El tiempo de configuración del entorno fue una fracción importante del
  esfuerzo total. En un proyecto real habría que valorar esas horas frente al costo de un servicio gestionado.
- **Medir antes de decidir.** Los experimentos (E1–E7) muestran que Spark es más lento que PostgreSQL en consultas
  pequeñas: sobredimensionar con un clúster distribuido tiene un costo sin beneficio hasta cierto volumen. El ×100 del
  volumen sí justificaría ese gasto ([04_justificacion_tecnologica.md](../tecnico/04_justificacion_tecnologica.md)).
- **Planificar las dependencias ahorra tiempo.** Las tareas bloqueadas entre parejas fueron el principal riesgo del
  cronograma; tenerlas escritas permitió adelantar trabajo con datos de prueba.

---

## 2. Aprendizaje Continuo (AC, nivel medio)

> *Capacidad para ejecutar estrategias con el fin de resolver sus necesidades educativas o de aprendizaje, de acuerdo
> con los objetivos o las intenciones de aprendizaje.*

### 2.1 Aplicación del atributo en la solución

El proyecto exigió aprender tecnologías y conceptos que no se cubren a fondo en clase:

| Tema | Qué hubo que aprender | Dónde se aplicó |
|---|---|---|
| PostgreSQL avanzado | Particionamiento por rango, BRIN frente a B-tree, vistas materializadas con `REFRESH CONCURRENTLY`, CTE recursivas, `EXPLAIN ANALYZE` | `sql/ddl/`, `sql/queries/`, [postgres_optimizacion.md](../resultados/postgres_optimizacion.md) |
| MongoDB | `$jsonSchema` con `validationLevel: moderate`, índices 2dsphere y de texto, Aggregation Framework (`$facet`, `$bucket`, `$setWindowFields`, `$lookup`, `$geoNear`) | `mongo/`, `conectores_mongo_spark.py` |
| HDFS y YARN | Bloques, réplicas, `fsck`, latidos y re-replicación, Hadoop Streaming con combiners | `hadoop/`, [hdfs_tolerancia.md](../resultados/hdfs_tolerancia.md) |
| Spark | DataFrames y Spark SQL, evaluación perezosa, particionamiento, *broadcast joins*, MLlib, Structured Streaming con *watermark* | `spark/jobs/` |
| Integración | JDBC desde Spark con *upsert* idempotente, conector Mongo–Spark, Docker Compose multi-servicio | `carga_pg.py`, `docker/` |

**Estrategias de aprendizaje que usamos:**

1. **Documentación oficial primero** (PostgreSQL, MongoDB, Hadoop, Spark), contrastada con pruebas pequeñas antes
   de escribir el código definitivo; por ejemplo, la prueba de humo `spark.range(1_000_000).count()` antes del ETL.
2. **Aprender midiendo:** en lugar de aceptar afirmaciones como «los índices aceleran» o «Spark escala», se diseñaron
   experimentos con hipótesis y se midió (A2.5, B2.5, B4.3, E1–E7). Algunos resultados contradijeron la intuición:
   BRIN ocupa 176 veces menos que B-tree (600 KB frente a 103 MB) pero fue más lento en un rango de 3 meses (51 frente a 42 ms).
3. **Pruebas automáticas como red de seguridad:** `tests/` detectó errores reales, como la lectura de cabeceras SQL
   que convertía un salto de línea en un parámetro fantasma.
4. **Registrar lo aprendido:** cada pareja escribió guías (`GUIA_SPARK.md`, `GUIA_POSTGRES_HADOOP.md`) para que la
   otra pudiera reproducir su parte, y los problemas resueltos quedaron documentados (puerto ocupado, Python en el
   NodeManager, compatibilidad con Python 3.5, PowerShell 5.1).
5. **Aprendizaje entre pares:** el contrato de datos obligó a cada pareja a entender lo suficiente de las tecnologías
   de la otra para integrarlas en el dashboard y defenderlas.

### 2.2 Impacto del proyecto en la sociedad

Las ciudades generan cada vez más datos (sensores, transporte, energía) y las tecnologías para gestionarlos cambian
rápido. Un profesional que sabe aprender una tecnología nueva de forma estructurada (documentación, prueba mínima,
medición, documentación propia) puede adaptar sistemas como este a nuevas fuentes, por ejemplo ruido, transporte
público o estacionamientos, sin rehacerlos desde cero. Además, documentar el proceso y publicarlo junto al código
permite que otros equipos, incluida una municipalidad, reutilicen lo aprendido.

### 2.3 Retroalimentación obtenida

- **Lo que funcionó:** probar en pequeño antes de escalar ahorró horas. La prueba a 100k filas del benchmark detectó
  errores de configuración antes de las corridas de 5 M.
- **Lo que costó:** las diferencias de versiones (Python 3.5 en la imagen de Hadoop, PowerShell 5.1 en Windows)
  exigieron aprender a leer los errores de capas que no se esperaban.
- **Lo que haríamos distinto:** levantar y validar el entorno completo, sobre todo el clúster de Hadoop con YARN, en
  la primera semana, porque varias tareas dependían de él.

