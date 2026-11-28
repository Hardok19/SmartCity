# 10. Conclusiones

## 10.1 Respuesta a la pregunta del proyecto

*¿Qué tecnología es la más apropiada para cada tipo de dato y cada necesidad de procesamiento dentro de una ciudad
inteligente?* Los experimentos muestran que **la respuesta depende del dato y de la consulta, no de la tecnología**:

1. **El catálogo y lo que se consulta en línea van en PostgreSQL.** Es imbatible en consultas puntuales (0,7 ms con
   5 M filas), en relaciones con integridad (JOIN ×410 más rápido que `$lookup`) y en la ingesta por lotes (`COPY` de
   5 M filas en 9,6 s). Su límite es escalar en vertical: por eso guarda lo limpio y agregado, no el histórico crudo.
2. **Lo que cambia de forma va en MongoDB.** Tipos de sensor y versiones de firmware nuevos entran sin migraciones, y
   los eventos e incidentes con arreglos y texto libre caben naturalmente en un documento. No es la herramienta para
   relacionar colecciones ni para recorrer todo el histórico en un solo nodo.
3. **El histórico crudo va en HDFS.** No consulta: guarda, replica y sobrevive a la caída de un nodo, y es la fuente
   para reprocesar.
4. **El procesamiento masivo va en Spark.** Es el único cuyo tiempo crece menos que el volumen; a 5 M filas ya superó a
   PostgreSQL en agregación y join, y mejoró ×1,9 al duplicar los núcleos. Su costo fijo lo descarta para consultas
   interactivas.

La frase central del enunciado se confirma con datos: *el objetivo no es usar cuatro tecnologías, es demostrar por qué
una arquitectura moderna puede necesitar tecnologías distintas para almacenar, procesar y analizar distintos tipos de
información.*

## 10.2 Lo que aprendimos al medir

- **Algunas intuiciones fallaron.** BRIN es 176 veces más chico que B-tree pero fue más lento en nuestras consultas;
  Spark fue ×11 más lento que PostgreSQL con 100k filas y más rápido con 5 M. Sin medir habríamos elegido mal en ambos
  casos.
- **El formato importa tanto como el motor.** Parte de la ventaja de Spark viene de leer 12 MB de Parquet columnar en
  lugar de 501 MB de tabla por filas. Elegir tecnología es también elegir cómo se guarda el dato.
- **Las consultas se diseñan para el motor.** El mismo join en MongoDB pasó de 237 s a 0,93 s agrupando antes del
  `$lookup`. En PostgreSQL esa decisión la toma el optimizador.
- **Los costos fijos importan.** Spark (~0,5 s por acción, ~12 s por job) y YARN (~17 s por job) solo se justifican con
  volumen.

## 10.3 Limitaciones

- Todo corre en una máquina: no se midió la escalabilidad horizontal real de MongoDB (sharding) ni de Spark (varios
  workers); E5 la aproxima con núcleos.
- OpenAQ solo aporta PM2.5 real para 2 de las 5 zonas en el período; el resto se modela, y siempre se marca con
  `origen`.
- Movilidad y Energía son simuladas: sus patrones son supuestos del generador, no hallazgos.
- El modelo predictivo es débil (R² ≤ 0,05): el clima solo no explica la contaminación.
- El NameNode no está en alta disponibilidad.

## 10.4 Trabajo futuro

- Incorporar fuentes reales de tráfico (por ejemplo, contadores de Transport for London) para el análisis de
  movilidad y como variable del modelo predictivo.
- Probar sharding en MongoDB y un segundo worker de Spark para medir la escalabilidad horizontal.
- Colecciones de series temporales de MongoDB y particionamiento automático por mes con retención hacia HDFS.
- Hive o Spark SQL directamente sobre `/smartcity/curated` para consultas ad hoc del histórico.
- Llevar el streaming a una fuente real (Kafka) y enviar las alertas a la municipalidad.
