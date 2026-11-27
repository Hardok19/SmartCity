# Optimización en PostgreSQL (A2.5)

Generado por `scripts/pg_optimizacion.py` el 2026-11-27 13:46 UTC sobre `medicion_horaria` con **175.440 filas** (24 particiones mensuales + DEFAULT). Mediana de 5 repeticiones tras 1 calentamiento; cada `EXPLAIN ANALYZE` corre en una transacción revertida (los `SET` y `DROP INDEX` de prueba no persisten).

| Caso | Variante | Tiempo (ms, mediana) |
|---|---|---:|
| A2.5-1 Últimas 50 lecturas de un sensor | sin_indice | 12,519 |
| A2.5-1 Últimas 50 lecturas de un sensor | con_indice_btree | 0,330 |
| A2.5-2 Promedio por variable de un mes | sin_poda_expresion | 20,110 |
| A2.5-2 Promedio por variable de un mes | con_poda_particiones | 3,256 |
| A2.5-3 Conteo y promedio de 3 meses | btree_timestamp | 5,113 |
| A2.5-3 Conteo y promedio de 3 meses | brin_timestamp | 11,082 |
| A2.5-3 Conteo y promedio de 3 meses | plan_elegido_por_postgres | 6,732 |
| A2.5-4 Mediciones por zona y día | consulta_en_vivo | 85,860 |
| A2.5-4 Mediciones por zona y día | vista_materializada | 0,520 |

Tamaño de los índices sobre `timestamp` (suma de las particiones): **BRIN 600 KB** frente a **B-tree 13.192 KB** (×22).

## A2.5-1 — Últimas 50 lecturas de un sensor

```sql
SELECT timestamp, variable, valor FROM medicion_horaria WHERE id_sensor = 'E-001' ORDER BY timestamp DESC LIMIT 50
```

**sin_indice** (12,519 ms)

```
              ->  Parallel Append  (cost=0.00..2979.56 rows=7311 width=24) (actual time=0.009..5.132 rows=5848 loops=3)
                    ->  Parallel Seq Scan on medicion_horaria_2023_01 medicion_horaria_1  (cost=0.00..124.71 rows=438 width=24) (actual time=0.010..0.706 rows=744 loops=1)
                          Rows Removed by Filter: 6696
                    ->  Parallel Seq Scan on medicion_horaria_2023_03 medicion_horaria_3  (cost=0.00..124.71 rows=438 width=24) (actual time=0.005..0.645 rows=744 loops=1)
                          Rows Removed by Filter: 6696
                    ->  Parallel Seq Scan on medicion_horaria_2023_05 medicion_horaria_5  (cost=0.00..124.71 rows=438 width=24) (actual time=0.010..0.736 rows=744 loops=1)
                          Rows Removed by Filter: 6696
                    ->  Parallel Seq Scan on medicion_horaria_2023_07 medicion_horaria_7  (cost=0.00..124.71 rows=438 width=24) (actual time=0.004..0.626 rows=744 loops=1)
                          Rows Removed by Filter: 6696
                    ->  Parallel Seq Scan on medicion_horaria_2023_08 medicion_horaria_8  (cost=0.00..124.71 rows=438 width=24) (actual time=0.006..0.719 rows=744 loops=1)
                          Rows Removed by Filter: 6696
                    ->  Parallel Seq Scan on medicion_horaria_2023_10 medicion_horaria_10  (cost=0.00..124.71 rows=438 width=24) (actual time=0.007..0.675 rows=744 loops=1)
                          Rows Removed by Filter: 6696
                    ->  Parallel Seq Scan on medicion_horaria_2023_12 medicion_horaria_12  (cost=0.00..124.71 rows=438 width=24) (actual time=0.007..0.657 rows=744 loops=1)
                          Rows Removed by Filter: 6696
                    ->  Parallel Seq Scan on medicion_horaria_2024_01 medicion_horaria_13  (cost=0.00..124.71 rows=438 width=24) (actual time=0.011..0.695 rows=744 loops=1)
                          Rows Removed by Filter: 6696
                    ->  Parallel Seq Scan on medicion_horaria_2024_03 medicion_horaria_15  (cost=0.00..124.71 rows=438 width=24) (actual time=0.005..0.661 rows=744 loops=1)
                          Rows Removed by Filter: 6696
                    ->  Parallel Seq Scan on medicion_horaria_2024_05 medicion_horaria_17  (cost=0.00..124.71 rows=438 width=24) (actual time=0.026..0.218 rows=248 loops=3)
                          Rows Removed by Filter: 2232
                    ->  Parallel Seq Scan on medicion_horaria_2024_07 medicion_horaria_19  (cost=0.00..124.71 rows=438 width=24) (actual time=0.004..0.291 rows=372 loops=2)
                          Rows Removed by Filter: 3348
                    ->  Parallel Seq Scan on medicion_horaria_2024_08 medicion_horaria_20  (cost=0.00..124.71 rows=438 width=24) (actual time=0.003..0.549 rows=744 loops=1)
                          Rows Removed by Filter: 6696
                    ->  Parallel Seq Scan on medicion_horaria_2024_10 medicion_horaria_22  (cost=0.00..124.71 rows=438 width=24) (actual time=0.003..0.531 rows=744 loops=1)
                          Rows Removed by Filter: 6696
                    ->  Parallel Seq Scan on medicion_horaria_2024_12 medicion_horaria_24  (cost=0.00..124.71 rows=438 width=24) (actual time=0.002..0.734 rows=744 loops=1)
                          Rows Removed by Filter: 6696
                    ->  Parallel Seq Scan on medicion_horaria_2023_04 medicion_horaria_4  (cost=0.00..120.94 rows=424 width=24) (actual time=0.003..0.510 rows=720 loops=1)
                          Rows Removed by Filter: 6480
                    ->  Parallel Seq Scan on medicion_horaria_2023_06 medicion_horaria_6  (cost=0.00..120.94 rows=424 width=24) (actual time=0.002..0.512 rows=720 loops=1)
                          Rows Removed by Filter: 6480
                    ->  Parallel Seq Scan on medicion_horaria_2023_09 medicion_horaria_9  (cost=0.00..120.94 rows=424 width=24) (actual time=0.002..0.509 rows=720 loops=1)
                          Rows Removed by Filter: 6480
                    ->  Parallel Seq Scan on medicion_horaria_2023_11 medicion_horaria_11  (cost=0.00..120.94 rows=424 width=24) (actual time=0.003..0.497 rows=720 loops=1)
                          Rows Removed by Filter: 6480
                    ->  Parallel Seq Scan on medicion_horaria_2024_04 medicion_horaria_16  (cost=0.00..120.94 rows=424 width=24) (actual time=0.003..0.517 rows=720 loops=1)
                          Rows Removed by Filter: 6480
                    ->  Parallel Seq Scan on medicion_horaria_2024_06 medicion_horaria_18  (cost=0.00..120.94 rows=424 width=24) (actual time=0.002..0.507 rows=720 loops=1)
                          Rows Removed by Filter: 6480
                    ->  Parallel Seq Scan on medicion_horaria_2024_09 medicion_horaria_21  (cost=0.00..120.94 rows=424 width=24) (actual time=0.002..0.535 rows=720 loops=1)
                          Rows Removed by Filter: 6480
                    ->  Parallel Seq Scan on medicion_horaria_2024_11 medicion_horaria_23  (cost=0.00..120.94 rows=424 width=24) (actual time=0.002..0.508 rows=720 loops=1)
                          Rows Removed by Filter: 6480
                    ->  Parallel Seq Scan on medicion_horaria_2024_02 medicion_horaria_14  (cost=0.00..117.18 rows=409 width=24) (actual time=0.002..0.506 rows=696 loops=1)
                          Rows Removed by Filter: 6264
                    ->  Parallel Seq Scan on medicion_horaria_2023_02 medicion_horaria_2  (cost=0.00..112.41 rows=395 width=24) (actual time=0.004..0.530 rows=672 loops=1)
                          Rows Removed by Filter: 6048
                    ->  Parallel Seq Scan on medicion_horaria_default medicion_horaria_25  (cost=0.00..0.00 rows=1 width=114) (actual time=0.001..0.001 rows=0 loops=1)
Execution Time: 12.735 ms
```

**con_indice_btree** (0,330 ms)

```
  ->  Merge Append  (cost=7.49..4253.26 rows=17545 width=24) (actual time=0.246..0.263 rows=50 loops=1)
        ->  Index Scan using medicion_horaria_2023_01_id_sensor_timestamp_idx on medicion_horaria_2023_01 medicion_horaria_1  (cost=0.28..157.99 rows=744 width=24) (actual time=0.018..0.018 rows=1 loops=1)
              Index Cond: ((id_sensor)::text = 'E-001'::text)
        ->  Index Scan using medicion_horaria_2023_02_id_sensor_timestamp_idx on medicion_horaria_2023_02 medicion_horaria_2  (cost=0.28..143.92 rows=672 width=24) (actual time=0.009..0.009 rows=1 loops=1)
              Index Cond: ((id_sensor)::text = 'E-001'::text)
        ->  Index Scan using medicion_horaria_2023_03_id_sensor_timestamp_idx on medicion_horaria_2023_03 medicion_horaria_3  (cost=0.28..161.99 rows=744 width=24) (actual time=0.008..0.008 rows=1 loops=1)
              Index Cond: ((id_sensor)::text = 'E-001'::text)
        ->  Index Scan using medicion_horaria_2023_04_id_sensor_timestamp_idx on medicion_horaria_2023_04 medicion_horaria_4  (cost=0.28..153.51 rows=720 width=24) (actual time=0.009..0.009 rows=1 loops=1)
              Index Cond: ((id_sensor)::text = 'E-001'::text)
        ->  Index Scan using medicion_horaria_2023_05_id_sensor_timestamp_idx on medicion_horaria_2023_05 medicion_horaria_5  (cost=0.28..157.99 rows=744 width=24) (actual time=0.010..0.010 rows=1 loops=1)
              Index Cond: ((id_sensor)::text = 'E-001'::text)
        ->  Index Scan using medicion_horaria_2023_06_id_sensor_timestamp_idx on medicion_horaria_2023_06 medicion_horaria_6  (cost=0.28..153.51 rows=720 width=24) (actual time=0.011..0.011 rows=1 loops=1)
              Index Cond: ((id_sensor)::text = 'E-001'::text)
        ->  Index Scan using medicion_horaria_2023_07_id_sensor_timestamp_idx on medicion_horaria_2023_07 medicion_horaria_7  (cost=0.28..157.99 rows=744 width=24) (actual time=0.007..0.007 rows=1 loops=1)
              Index Cond: ((id_sensor)::text = 'E-001'::text)
        ->  Index Scan using medicion_horaria_2023_08_id_sensor_timestamp_idx on medicion_horaria_2023_08 medicion_horaria_8  (cost=0.28..157.99 rows=744 width=24) (actual time=0.011..0.011 rows=1 loops=1)
              Index Cond: ((id_sensor)::text = 'E-001'::text)
        ->  Index Scan using medicion_horaria_2023_09_id_sensor_timestamp_idx on medicion_horaria_2023_09 medicion_horaria_9  (cost=0.28..153.51 rows=720 width=24) (actual time=0.011..0.011 rows=1 loops=1)
              Index Cond: ((id_sensor)::text = 'E-001'::text)
        ->  Index Scan using medicion_horaria_2023_10_id_sensor_timestamp_idx on medicion_horaria_2023_10 medicion_horaria_10  (cost=0.28..157.99 rows=744 width=24) (actual time=0.011..0.011 rows=1 loops=1)
              Index Cond: ((id_sensor)::text = 'E-001'::text)
        ->  Index Scan using medicion_horaria_2023_11_id_sensor_timestamp_idx on medicion_horaria_2023_11 medicion_horaria_11  (cost=0.28..153.51 rows=720 width=24) (actual time=0.011..0.011 rows=1 loops=1)
              Index Cond: ((id_sensor)::text = 'E-001'::text)
        ->  Index Scan using medicion_horaria_2023_12_id_sensor_timestamp_idx on medicion_horaria_2023_12 medicion_horaria_12  (cost=0.28..157.99 rows=744 width=24) (actual time=0.009..0.009 rows=1 loops=1)
              Index Cond: ((id_sensor)::text = 'E-001'::text)
        ->  Index Scan using medicion_horaria_2024_01_id_sensor_timestamp_idx on medicion_horaria_2024_01 medicion_horaria_13  (cost=0.28..157.99 rows=744 width=24) (actual time=0.008..0.008 rows=1 loops=1)
              Index Cond: ((id_sensor)::text = 'E-001'::text)
        ->  Index Scan using medicion_horaria_2024_02_id_sensor_timestamp_idx on medicion_horaria_2024_02 medicion_horaria_14  (cost=0.28..153.59 rows=696 width=24) (actual time=0.007..0.007 rows=1 loops=1)
              Index Cond: ((id_sensor)::text = 'E-001'::text)
        ->  Index Scan using medicion_horaria_2024_03_id_sensor_timestamp_idx on medicion_horaria_2024_03 medicion_horaria_15  (cost=0.28..161.99 rows=744 width=24) (actual time=0.010..0.010 rows=1 loops=1)
              Index Cond: ((id_sensor)::text = 'E-001'::text)
        ->  Index Scan using medicion_horaria_2024_04_id_sensor_timestamp_idx on medicion_horaria_2024_04 medicion_horaria_16  (cost=0.28..153.51 rows=720 width=24) (actual time=0.010..0.010 rows=1 loops=1)
              Index Cond: ((id_sensor)::text = 'E-001'::text)
        ->  Index Scan using medicion_horaria_2024_05_id_sensor_timestamp_idx on medicion_horaria_2024_05 medicion_horaria_17  (cost=0.28..157.99 rows=744 width=24) (actual time=0.011..0.011 rows=1 loops=1)
              Index Cond: ((id_sensor)::text = 'E-001'::text)
        ->  Index Scan using medicion_horaria_2024_06_id_sensor_timestamp_idx on medicion_horaria_2024_06 medicion_horaria_18  (cost=0.28..153.51 rows=720 width=24) (actual time=0.011..0.011 rows=1 loops=1)
              Index Cond: ((id_sensor)::text = 'E-001'::text)
        ->  Index Scan using medicion_horaria_2024_07_id_sensor_timestamp_idx on medicion_horaria_2024_07 medicion_horaria_19  (cost=0.28..157.99 rows=744 width=24) (actual time=0.009..0.009 rows=1 loops=1)
              Index Cond: ((id_sensor)::text = 'E-001'::text)
        ->  Index Scan using medicion_horaria_2024_08_id_sensor_timestamp_idx on medicion_horaria_2024_08 medicion_horaria_20  (cost=0.28..161.99 rows=744 width=24) (actual time=0.007..0.007 rows=1 loops=1)
              Index Cond: ((id_sensor)::text = 'E-001'::text)
        ->  Index Scan using medicion_horaria_2024_09_id_sensor_timestamp_idx on medicion_horaria_2024_09 medicion_horaria_21  (cost=0.28..153.51 rows=720 width=24) (actual time=0.011..0.011 rows=1 loops=1)
              Index Cond: ((id_sensor)::text = 'E-001'::text)
        ->  Index Scan using medicion_horaria_2024_10_id_sensor_timestamp_idx on medicion_horaria_2024_10 medicion_horaria_22  (cost=0.28..157.99 rows=744 width=24) (actual time=0.010..0.010 rows=1 loops=1)
              Index Cond: ((id_sensor)::text = 'E-001'::text)
        ->  Index Scan using medicion_horaria_2024_11_id_sensor_timestamp_idx on medicion_horaria_2024_11 medicion_horaria_23  (cost=0.28..153.51 rows=720 width=24) (actual time=0.010..0.010 rows=1 loops=1)
              Index Cond: ((id_sensor)::text = 'E-001'::text)
        ->  Index Scan using medicion_horaria_2024_12_id_sensor_timestamp_idx on medicion_horaria_2024_12 medicion_horaria_24  (cost=0.28..157.99 rows=744 width=24) (actual time=0.011..0.022 rows=50 loops=1)
              Index Cond: ((id_sensor)::text = 'E-001'::text)
        ->  Index Scan using medicion_horaria_default_id_sensor_timestamp_idx on medicion_horaria_default medicion_horaria_25  (cost=0.12..8.14 rows=1 width=114) (actual time=0.001..0.001 rows=0 loops=1)
              Index Cond: ((id_sensor)::text = 'E-001'::text)
Execution Time: 0.316 ms
```

## A2.5-2 — Promedio por variable de un mes

```sql
SELECT variable, AVG(valor) FROM medicion_horaria WHERE timestamp >= '2024-12-01'::timestamptz AND timestamp < '2024-12-01'::timestamptz + interval '1 month' GROUP BY variable
```

**sin_poda_expresion** (20,110 ms)

```
                    ->  Parallel Append  (cost=0.00..3460.87 rows=374 width=16) (actual time=12.281..13.062 rows=2480 loops=3)
                          ->  Parallel Seq Scan on medicion_horaria_2023_01 medicion_horaria_1  (cost=0.00..146.59 rows=22 width=16) (actual time=1.481..1.481 rows=0 loops=1)
                                Rows Removed by Filter: 7440
                          ->  Parallel Seq Scan on medicion_horaria_2023_03 medicion_horaria_3  (cost=0.00..146.59 rows=22 width=16) (actual time=1.983..1.983 rows=0 loops=1)
                                Rows Removed by Filter: 7440
                          ->  Parallel Seq Scan on medicion_horaria_2023_05 medicion_horaria_5  (cost=0.00..146.59 rows=22 width=16) (actual time=1.322..1.322 rows=0 loops=1)
                                Rows Removed by Filter: 7440
                          ->  Parallel Seq Scan on medicion_horaria_2023_07 medicion_horaria_7  (cost=0.00..146.59 rows=22 width=16) (actual time=1.559..1.559 rows=0 loops=1)
                                Rows Removed by Filter: 7440
                          ->  Parallel Seq Scan on medicion_horaria_2023_08 medicion_horaria_8  (cost=0.00..146.59 rows=22 width=16) (actual time=2.001..2.001 rows=0 loops=1)
                                Rows Removed by Filter: 7440
                          ->  Parallel Seq Scan on medicion_horaria_2023_10 medicion_horaria_10  (cost=0.00..146.59 rows=22 width=16) (actual time=1.673..1.673 rows=0 loops=1)
                                Rows Removed by Filter: 7440
                          ->  Parallel Seq Scan on medicion_horaria_2023_12 medicion_horaria_12  (cost=0.00..146.59 rows=22 width=16) (actual time=1.663..1.663 rows=0 loops=1)
                                Rows Removed by Filter: 7440
                          ->  Parallel Seq Scan on medicion_horaria_2024_01 medicion_horaria_13  (cost=0.00..146.59 rows=22 width=16) (actual time=1.300..1.300 rows=0 loops=1)
                                Rows Removed by Filter: 7440
                          ->  Parallel Seq Scan on medicion_horaria_2024_03 medicion_horaria_15  (cost=0.00..146.59 rows=22 width=16) (actual time=1.420..1.420 rows=0 loops=1)
                                Rows Removed by Filter: 7440
                          ->  Parallel Seq Scan on medicion_horaria_2024_05 medicion_horaria_17  (cost=0.00..146.59 rows=22 width=16) (actual time=1.321..1.321 rows=0 loops=1)
                                Rows Removed by Filter: 7440
                          ->  Parallel Seq Scan on medicion_horaria_2024_07 medicion_horaria_19  (cost=0.00..146.59 rows=22 width=16) (actual time=1.539..1.539 rows=0 loops=1)
                                Rows Removed by Filter: 7440
                          ->  Parallel Seq Scan on medicion_horaria_2024_08 medicion_horaria_20  (cost=0.00..146.59 rows=22 width=16) (actual time=1.404..1.404 rows=0 loops=1)
                                Rows Removed by Filter: 7440
                          ->  Parallel Seq Scan on medicion_horaria_2024_10 medicion_horaria_22  (cost=0.00..146.59 rows=22 width=16) (actual time=1.438..1.438 rows=0 loops=1)
                                Rows Removed by Filter: 7440
                          ->  Parallel Seq Scan on medicion_horaria_2024_12 medicion_horaria_24  (cost=0.00..146.59 rows=22 width=16) (actual time=0.005..0.632 rows=2480 loops=3)
                          ->  Parallel Seq Scan on medicion_horaria_2023_04 medicion_horaria_4  (cost=0.00..142.12 rows=21 width=16) (actual time=0.727..0.727 rows=0 loops=2)
                                Rows Removed by Filter: 3600
                          ->  Parallel Seq Scan on medicion_horaria_2023_06 medicion_horaria_6  (cost=0.00..142.12 rows=21 width=16) (actual time=1.725..1.725 rows=0 loops=1)
                                Rows Removed by Filter: 7200
                          ->  Parallel Seq Scan on medicion_horaria_2023_09 medicion_horaria_9  (cost=0.00..142.12 rows=21 width=16) (actual time=1.739..1.739 rows=0 loops=1)
                                Rows Removed by Filter: 7200
                          ->  Parallel Seq Scan on medicion_horaria_2023_11 medicion_horaria_11  (cost=0.00..142.12 rows=21 width=16) (actual time=1.711..1.711 rows=0 loops=1)
                                Rows Removed by Filter: 7200
                          ->  Parallel Seq Scan on medicion_horaria_2024_04 medicion_horaria_16  (cost=0.00..142.12 rows=21 width=16) (actual time=1.794..1.794 rows=0 loops=1)
                                Rows Removed by Filter: 7200
                          ->  Parallel Seq Scan on medicion_horaria_2024_06 medicion_horaria_18  (cost=0.00..142.12 rows=21 width=16) (actual time=2.067..2.067 rows=0 loops=1)
                                Rows Removed by Filter: 7200
                          ->  Parallel Seq Scan on medicion_horaria_2024_09 medicion_horaria_21  (cost=0.00..142.12 rows=21 width=16) (actual time=1.697..1.697 rows=0 loops=1)
                                Rows Removed by Filter: 7200
                          ->  Parallel Seq Scan on medicion_horaria_2024_11 medicion_horaria_23  (cost=0.00..142.12 rows=21 width=16) (actual time=1.716..1.716 rows=0 loops=1)
                                Rows Removed by Filter: 7200
                          ->  Parallel Seq Scan on medicion_horaria_2024_02 medicion_horaria_14  (cost=0.00..137.65 rows=21 width=16) (actual time=1.512..1.512 rows=0 loops=1)
                                Rows Removed by Filter: 6960
                          ->  Parallel Seq Scan on medicion_horaria_2023_02 medicion_horaria_2  (cost=0.00..132.18 rows=20 width=16) (actual time=1.290..1.290 rows=0 loops=1)
                                Rows Removed by Filter: 6720
                          ->  Parallel Seq Scan on medicion_horaria_default medicion_horaria_25  (cost=0.00..0.00 rows=1 width=106) (actual time=0.001..0.001 rows=0 loops=1)
Execution Time: 21.607 ms
```

**con_poda_particiones** (3,256 ms)

```
  ->  Append  (cost=0.00..237.40 rows=7440 width=16) (actual time=0.006..2.365 rows=7440 loops=1)
        Subplans Removed: 1
        ->  Seq Scan on medicion_horaria_2024_12 medicion_horaria_1  (cost=0.00..200.20 rows=7439 width=16) (actual time=0.005..1.869 rows=7440 loops=1)
Execution Time: 3.766 ms
```

## A2.5-3 — Conteo y promedio de 3 meses

```sql
SELECT COUNT(*), AVG(valor) FROM medicion_horaria WHERE timestamp >= '2024-12-01'::timestamptz - interval '3 months' AND timestamp < '2024-12-01'::timestamptz
```

**btree_timestamp** (5,113 ms)

```
  ->  Append  (cost=0.29..1228.47 rows=21861 width=8) (actual time=0.139..4.037 rows=21840 loops=1)
        Subplans Removed: 21
        ->  Bitmap Heap Scan on medicion_horaria_2024_09 medicion_horaria_1  (cost=118.08..312.08 rows=7200 width=8) (actual time=0.139..0.819 rows=7200 loops=1)
              Recheck Cond: (("timestamp" >= ('2024-12-01 00:00:00+00'::timestamp with time zone - '3 mons'::interval)) AND ("timestamp" < '2024-12-01 00:00:00+00'::timestamp with time zone))
              Heap Blocks: exact=68
              ->  Bitmap Index Scan on medicion_horaria_2024_09_timestamp_idx1  (cost=0.00..116.28 rows=7200 width=0) (actual time=0.128..0.128 rows=7200 loops=1)
                    Index Cond: (("timestamp" >= ('2024-12-01 00:00:00+00'::timestamp with time zone - '3 mons'::interval)) AND ("timestamp" < '2024-12-01 00:00:00+00'::timestamp with time zone))
        ->  Bitmap Heap Scan on medicion_horaria_2024_10 medicion_horaria_2  (cost=120.55..320.75 rows=7440 width=8) (actual time=0.141..0.839 rows=7440 loops=1)
              Recheck Cond: (("timestamp" >= ('2024-12-01 00:00:00+00'::timestamp with time zone - '3 mons'::interval)) AND ("timestamp" < '2024-12-01 00:00:00+00'::timestamp with time zone))
              Heap Blocks: exact=70
              ->  Bitmap Index Scan on medicion_horaria_2024_10_timestamp_idx1  (cost=0.00..118.69 rows=7440 width=0) (actual time=0.133..0.133 rows=7440 loops=1)
                    Index Cond: (("timestamp" >= ('2024-12-01 00:00:00+00'::timestamp with time zone - '3 mons'::interval)) AND ("timestamp" < '2024-12-01 00:00:00+00'::timestamp with time zone))
        ->  Bitmap Heap Scan on medicion_horaria_2024_11 medicion_horaria_3  (cost=118.08..312.08 rows=7200 width=8) (actual time=0.240..1.089 rows=7200 loops=1)
              Recheck Cond: (("timestamp" >= ('2024-12-01 00:00:00+00'::timestamp with time zone - '3 mons'::interval)) AND ("timestamp" < '2024-12-01 00:00:00+00'::timestamp with time zone))
              Heap Blocks: exact=68
              ->  Bitmap Index Scan on medicion_horaria_2024_11_timestamp_idx1  (cost=0.00..116.28 rows=7200 width=0) (actual time=0.232..0.232 rows=7200 loops=1)
                    Index Cond: (("timestamp" >= ('2024-12-01 00:00:00+00'::timestamp with time zone - '3 mons'::interval)) AND ("timestamp" < '2024-12-01 00:00:00+00'::timestamp with time zone))
Execution Time: 5.388 ms
```

**brin_timestamp** (11,082 ms)

```
              ->  Parallel Append  (cost=13.83..3648.42 rows=9121 width=8) (actual time=0.291..3.311 rows=7280 loops=3)
                    Subplans Removed: 21
                    ->  Parallel Bitmap Heap Scan on medicion_horaria_2024_10 medicion_horaria_2  (cost=13.89..160.48 rows=4376 width=8) (actual time=0.049..0.882 rows=2480 loops=3)
                          Recheck Cond: (("timestamp" >= ('2024-12-01 00:00:00+00'::timestamp with time zone - '3 mons'::interval)) AND ("timestamp" < '2024-12-01 00:00:00+00'::timestamp with time zone))
                          Heap Blocks: lossy=16
                          ->  Bitmap Index Scan on medicion_horaria_2024_10_timestamp_idx  (cost=0.00..12.03 rows=7440 width=0) (actual time=0.107..0.107 rows=700 loops=1)
                                Index Cond: (("timestamp" >= ('2024-12-01 00:00:00+00'::timestamp with time zone - '3 mons'::interval)) AND ("timestamp" < '2024-12-01 00:00:00+00'::timestamp with time zone))
                    ->  Parallel Bitmap Heap Scan on medicion_horaria_2024_09 medicion_horaria_1  (cost=13.83..155.95 rows=4235 width=8) (actual time=0.047..1.263 rows=3600 loops=2)
                          Recheck Cond: (("timestamp" >= ('2024-12-01 00:00:00+00'::timestamp with time zone - '3 mons'::interval)) AND ("timestamp" < '2024-12-01 00:00:00+00'::timestamp with time zone))
                          Heap Blocks: lossy=52
                          ->  Bitmap Index Scan on medicion_horaria_2024_09_timestamp_idx  (cost=0.00..12.03 rows=7200 width=0) (actual time=0.039..0.039 rows=680 loops=1)
                                Index Cond: (("timestamp" >= ('2024-12-01 00:00:00+00'::timestamp with time zone - '3 mons'::interval)) AND ("timestamp" < '2024-12-01 00:00:00+00'::timestamp with time zone))
                    ->  Parallel Bitmap Heap Scan on medicion_horaria_2024_11 medicion_horaria_3  (cost=13.83..155.95 rows=4235 width=8) (actual time=0.700..2.888 rows=7200 loops=1)
                          Recheck Cond: (("timestamp" >= ('2024-12-01 00:00:00+00'::timestamp with time zone - '3 mons'::interval)) AND ("timestamp" < '2024-12-01 00:00:00+00'::timestamp with time zone))
                          Heap Blocks: lossy=68
                          ->  Bitmap Index Scan on medicion_horaria_2024_11_timestamp_idx  (cost=0.00..12.03 rows=7200 width=0) (actual time=0.688..0.688 rows=680 loops=1)
                                Index Cond: (("timestamp" >= ('2024-12-01 00:00:00+00'::timestamp with time zone - '3 mons'::interval)) AND ("timestamp" < '2024-12-01 00:00:00+00'::timestamp with time zone))
Execution Time: 11.264 ms
```

**plan_elegido_por_postgres** (6,732 ms)

```
  ->  Append  (cost=0.29..3756.99 rows=21858 width=8) (actual time=0.004..5.390 rows=21840 loops=1)
        Subplans Removed: 21
        ->  Seq Scan on medicion_horaria_2024_09 medicion_horaria_1  (cost=0.00..194.00 rows=7199 width=8) (actual time=0.004..1.393 rows=7200 loops=1)
        ->  Seq Scan on medicion_horaria_2024_10 medicion_horaria_2  (cost=0.00..200.20 rows=7439 width=8) (actual time=0.004..1.406 rows=7440 loops=1)
        ->  Seq Scan on medicion_horaria_2024_11 medicion_horaria_3  (cost=0.00..194.00 rows=7199 width=8) (actual time=0.004..1.342 rows=7200 loops=1)
Execution Time: 6.658 ms
```

## A2.5-4 — Mediciones por zona y día

```sql
SELECT zona, fecha, mediciones FROM mv_kpi_diario_zona
```

**consulta_en_vivo** (85,860 ms)

```
                                ->  Parallel Append  (cost=0.00..3050.51 rows=73101 width=14) (actual time=0.008..10.186 rows=58480 loops=3)
                                      ->  Parallel Seq Scan on medicion_horaria_2023_01 m_1  (cost=0.00..113.76 rows=4376 width=14) (actual time=0.009..0.887 rows=7440 loops=1)
                                      ->  Parallel Seq Scan on medicion_horaria_2023_03 m_3  (cost=0.00..113.76 rows=4376 width=14) (actual time=0.008..1.060 rows=7440 loops=1)
                                      ->  Parallel Seq Scan on medicion_horaria_2023_05 m_5  (cost=0.00..113.76 rows=4376 width=14) (actual time=0.005..0.730 rows=7440 loops=1)
                                      ->  Parallel Seq Scan on medicion_horaria_2023_07 m_7  (cost=0.00..113.76 rows=4376 width=14) (actual time=0.006..0.850 rows=7440 loops=1)
                                      ->  Parallel Seq Scan on medicion_horaria_2023_08 m_8  (cost=0.00..113.76 rows=4376 width=14) (actual time=0.004..0.973 rows=7440 loops=1)
                                      ->  Parallel Seq Scan on medicion_horaria_2023_10 m_10  (cost=0.00..113.76 rows=4376 width=14) (actual time=0.007..0.986 rows=7440 loops=1)
                                      ->  Parallel Seq Scan on medicion_horaria_2023_12 m_12  (cost=0.00..113.76 rows=4376 width=14) (actual time=0.007..0.723 rows=7440 loops=1)
                                      ->  Parallel Seq Scan on medicion_horaria_2024_01 m_13  (cost=0.00..113.76 rows=4376 width=14) (actual time=0.009..1.004 rows=7440 loops=1)
                                      ->  Parallel Seq Scan on medicion_horaria_2024_03 m_15  (cost=0.00..113.76 rows=4376 width=14) (actual time=0.005..0.762 rows=7440 loops=1)
                                      ->  Parallel Seq Scan on medicion_horaria_2024_05 m_17  (cost=0.00..113.76 rows=4376 width=14) (actual time=0.007..0.812 rows=7440 loops=1)
                                      ->  Parallel Seq Scan on medicion_horaria_2024_07 m_19  (cost=0.00..113.76 rows=4376 width=14) (actual time=0.006..0.983 rows=7440 loops=1)
                                      ->  Parallel Seq Scan on medicion_horaria_2024_08 m_20  (cost=0.00..113.76 rows=4376 width=14) (actual time=0.007..0.686 rows=7440 loops=1)
                                      ->  Parallel Seq Scan on medicion_horaria_2024_10 m_22  (cost=0.00..113.76 rows=4376 width=14) (actual time=0.007..0.896 rows=7440 loops=1)
                                      ->  Parallel Seq Scan on medicion_horaria_2024_12 m_24  (cost=0.00..113.76 rows=4376 width=14) (actual time=0.006..0.689 rows=7440 loops=1)
                                      ->  Parallel Seq Scan on medicion_horaria_2023_04 m_4  (cost=0.00..110.35 rows=4235 width=14) (actual time=0.004..0.256 rows=2400 loops=3)
                                      ->  Parallel Seq Scan on medicion_horaria_2023_06 m_6  (cost=0.00..110.35 rows=4235 width=14) (actual time=0.004..0.346 rows=3600 loops=2)
                                      ->  Parallel Seq Scan on medicion_horaria_2023_09 m_9  (cost=0.00..110.35 rows=4235 width=14) (actual time=0.003..0.720 rows=7200 loops=1)
                                      ->  Parallel Seq Scan on medicion_horaria_2023_11 m_11  (cost=0.00..110.35 rows=4235 width=14) (actual time=0.003..0.666 rows=7200 loops=1)
                                      ->  Parallel Seq Scan on medicion_horaria_2024_04 m_16  (cost=0.00..110.35 rows=4235 width=14) (actual time=0.003..0.679 rows=7200 loops=1)
                                      ->  Parallel Seq Scan on medicion_horaria_2024_06 m_18  (cost=0.00..110.35 rows=4235 width=14) (actual time=0.003..0.708 rows=7200 loops=1)
                                      ->  Parallel Seq Scan on medicion_horaria_2024_09 m_21  (cost=0.00..110.35 rows=4235 width=14) (actual time=0.003..0.741 rows=7200 loops=1)
                                      ->  Parallel Seq Scan on medicion_horaria_2024_11 m_23  (cost=0.00..110.35 rows=4235 width=14) (actual time=0.003..0.792 rows=7200 loops=1)
                                      ->  Parallel Seq Scan on medicion_horaria_2024_02 m_14  (cost=0.00..106.94 rows=4094 width=14) (actual time=0.002..0.715 rows=6960 loops=1)
                                      ->  Parallel Seq Scan on medicion_horaria_2023_02 m_2  (cost=0.00..102.53 rows=3953 width=14) (actual time=0.006..0.581 rows=6720 loops=1)
                                      ->  Parallel Seq Scan on medicion_horaria_default m_25  (cost=0.00..0.00 rows=1 width=66) (actual time=0.001..0.001 rows=0 loops=1)
                                      ->  Seq Scan on sensores s  (cost=0.00..2.50 rows=50 width=10) (actual time=0.010..0.030 rows=50 loops=3)
                                ->  Seq Scan on ubicaciones u  (cost=0.00..11.60 rows=160 width=222) (actual time=0.009..0.010 rows=5 loops=3)
Execution Time: 94.722 ms
```

**vista_materializada** (0,520 ms)

```
Seq Scan on mv_kpi_diario_zona  (cost=0.00..60.55 rows=3655 width=22) (actual time=0.005..0.428 rows=3655 loops=1)
Execution Time: 0.580 ms
```
