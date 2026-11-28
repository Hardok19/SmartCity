# Experimento de índices en MongoDB (B2.5 / E7)

Generado por `scripts/mongo_experimento_indices.py` el 2026-11-27 23:34 UTC. `lectura` = 1.068.584 documentos. Mediana de 3 repeticiones tras 1 calentamiento; «sin índice» = `hint({$natural: 1})` (recorrido completo forzado).

| Consulta | Variante | Índice esperado | Etapa | Docs examinados | Claves examinadas | Devueltos | Tiempo (ms) |
|---|---|---|---|---:|---:|---:|---:|
| MG-Q01 | con_indice | ix_sensor_ts | IXSCAN | 50 | 50 | 50 | 0 |
| MG-Q01 | sin_indice | ix_sensor_ts | COLLSCAN | 1.068.584 | 0 | 50 | 533 |
| MG-Q02 | con_indice | ix_tipo_zona_ts / ix_ts | IXSCAN | 1.550 | 1.550 | 336 | 3 |
| MG-Q02 | sin_indice | ix_tipo_zona_ts / ix_ts | COLLSCAN | 1.068.584 | 0 | 336 | 682 |
| MG-Q05 | con_indice | ix_zona_emitida | COLLSCAN | 1.190 | 0 | 347 | 1 |
| MG-Q05 | sin_indice | ix_zona_emitida | COLLSCAN | 1.190 | 0 | 347 | 1 |

## Lectura de resultados

- **MG-Q01** (Lecturas de un sensor (S-011), últimas 50): con índice examina 50 documentos frente a 1.068.584 sin índice (×21.371,7); tiempo ×533,0.
- **MG-Q02** (Lecturas de Zona Norte del 2024-04-01): con índice examina 1.550 documentos frente a 1.068.584 sin índice (×689,4); tiempo ×227,3.
- **MG-Q05** (Alertas por zona entre 2024-02-15 y 2024-06-15): con índice examina 1.190 documentos frente a 1.190 sin índice (×1,0); tiempo ×1,0.
  MongoDB no usa `ix_zona_emitida` (`zona.nombre`, `emitida`) porque el filtro es solo un rango de `emitida`: un índice compuesto sirve únicamente si la consulta filtra por su primer campo (regla del prefijo). Con 1.190 alertas el recorrido completo cuesta lo mismo que un índice (~1 ms); si la colección creciera, convendría un índice propio sobre `emitida` o filtrar también por zona.
