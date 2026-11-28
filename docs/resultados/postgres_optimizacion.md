# Optimización en PostgreSQL (A2.5)

Generado por `scripts/pg_optimizacion.py` el 2026-11-27 23:35 UTC sobre `medicion_horaria` con **2.632.112 filas** (24 particiones mensuales + DEFAULT). Mediana de 5 repeticiones tras 1 calentamiento; cada `EXPLAIN ANALYZE` corre en una transacción revertida (los `SET` y `DROP INDEX` de prueba no persisten).

| Caso | Variante | Tiempo (ms, mediana) |
|---|---|---:|
| A2.5-1 Últimas 50 lecturas de un sensor | sin_indice | 114,289 |
| A2.5-1 Últimas 50 lecturas de un sensor | con_indice_btree | 0,404 |
| A2.5-2 Promedio por variable de un mes | sin_poda_expresion | 254,525 |
| A2.5-2 Promedio por variable de un mes | con_poda_particiones | 24,838 |
| A2.5-3 Conteo y promedio de 3 meses | btree_timestamp | 41,796 |
| A2.5-3 Conteo y promedio de 3 meses | brin_timestamp | 50,980 |
| A2.5-3 Conteo y promedio de 3 meses | plan_elegido_por_postgres | 54,171 |
| A2.5-4 Mediciones por zona y día | consulta_en_vivo | 1.854,426 |
| A2.5-4 Mediciones por zona y día | vista_materializada | 0,472 |

Tamaño de los índices sobre `timestamp` (suma de las particiones): **BRIN 600 KB** frente a **B-tree 105.752 KB** (×176).

## A2.5-1 — Últimas 50 lecturas de un sensor

```sql
SELECT timestamp, variable, valor FROM medicion_horaria WHERE id_sensor = 'S-032' ORDER BY timestamp DESC LIMIT 50
```

**sin_indice** (114,289 ms)

```
              ->  Parallel Append  (cost=0.00..44467.85 rows=35219 width=22) (actual time=8.646..99.600 rows=28190 loops=3)
                    ->  Seq Scan on medicion_horaria_default medicion_horaria_25  (cost=0.00..0.00 rows=1 width=114) (actual time=0.012..0.012 rows=0 loops=1)
                    ->  Parallel Seq Scan on medicion_horaria_2024_05 medicion_horaria_17  (cost=0.00..2093.23 rows=2643 width=22) (actual time=10.292..13.955 rows=4434 loops=1)
                          Rows Removed by Filter: 120037
                    ->  Parallel Seq Scan on medicion_horaria_2024_03 medicion_horaria_15  (cost=0.00..2078.90 rows=2550 width=22) (actual time=10.063..14.742 rows=4428 loops=1)
                          Rows Removed by Filter: 119182
                    ->  Parallel Seq Scan on medicion_horaria_2024_07 medicion_horaria_19  (cost=0.00..2068.35 rows=2562 width=22) (actual time=12.973..16.807 rows=4410 loops=1)
                          Rows Removed by Filter: 118717
                    ->  Parallel Seq Scan on medicion_horaria_2024_04 medicion_horaria_16  (cost=0.00..2040.77 rows=2490 width=22) (actual time=9.818..13.298 rows=4320 loops=1)
                          Rows Removed by Filter: 117097
                    ->  Parallel Seq Scan on medicion_horaria_2024_10 medicion_horaria_22  (cost=0.00..2024.12 rows=2714 width=22) (actual time=12.732..19.295 rows=4464 loops=1)
                          Rows Removed by Filter: 116184
                    ->  Parallel Seq Scan on medicion_horaria_2024_08 medicion_horaria_20  (cost=0.00..2015.57 rows=2732 width=22) (actual time=10.755..15.044 rows=4464 loops=1)
                          Rows Removed by Filter: 115565
                    ->  Parallel Seq Scan on medicion_horaria_2024_06 medicion_horaria_18  (cost=0.00..2009.90 rows=2588 width=22) (actual time=11.200..15.292 rows=4320 loops=1)
                          Rows Removed by Filter: 115211
                    ->  Parallel Seq Scan on medicion_horaria_2024_12 medicion_horaria_24  (cost=0.00..1989.95 rows=2587 width=22) (actual time=9.114..12.767 rows=4434 loops=1)
                          Rows Removed by Filter: 114015
                    ->  Parallel Seq Scan on medicion_horaria_2024_09 medicion_horaria_21  (cost=0.00..1950.02 rows=2539 width=22) (actual time=9.594..12.814 rows=4320 loops=1)
                          Rows Removed by Filter: 111827
                    ->  Parallel Seq Scan on medicion_horaria_2024_11 medicion_horaria_23  (cost=0.00..1930.92 rows=2499 width=22) (actual time=11.479..15.304 rows=4308 loops=1)
                          Rows Removed by Filter: 110737
                    ->  Parallel Seq Scan on medicion_horaria_2024_01 medicion_horaria_13  (cost=0.00..1811.94 rows=1719 width=22) (actual time=10.630..14.181 rows=2944 loops=1)
                          Rows Removed by Filter: 104624
                    ->  Parallel Seq Scan on medicion_horaria_2023_12 medicion_horaria_12  (cost=0.00..1801.28 rows=1749 width=22) (actual time=10.996..14.402 rows=2976 loops=1)
                          Rows Removed by Filter: 103958
                    ->  Parallel Seq Scan on medicion_horaria_2023_07 medicion_horaria_7  (cost=0.00..1788.85 rows=1693 width=23) (actual time=3.684..4.612 rows=984 loops=3)
                          Rows Removed by Filter: 34414
                    ->  Parallel Seq Scan on medicion_horaria_2023_08 medicion_horaria_8  (cost=0.00..1781.39 rows=1666 width=23) (actual time=3.545..4.878 rows=1488 loops=2)
                          Rows Removed by Filter: 51374
                    ->  Parallel Seq Scan on medicion_horaria_2023_11 medicion_horaria_11  (cost=0.00..1758.46 rows=1692 width=22) (actual time=6.733..8.766 rows=2880 loops=1)
                          Rows Removed by Filter: 101495
                    ->  Parallel Seq Scan on medicion_horaria_2024_02 medicion_horaria_14  (cost=0.00..1753.85 rows=1722 width=22) (actual time=6.261..8.624 rows=2784 loops=1)
                          Rows Removed by Filter: 101372
                    ->  Parallel Seq Scan on medicion_horaria_2023_10 medicion_horaria_10  (cost=0.00..1747.25 rows=1756 width=23) (actual time=6.853..9.061 rows=2976 loops=1)
                          Rows Removed by Filter: 100554
                    ->  Parallel Seq Scan on medicion_horaria_2023_05 medicion_horaria_5  (cost=0.00..1738.58 rows=1768 width=22) (actual time=7.361..9.482 rows=2976 loops=1)
                          Rows Removed by Filter: 100327
                    ->  Parallel Seq Scan on medicion_horaria_2023_06 medicion_horaria_6  (cost=0.00..1717.71 rows=1688 width=22) (actual time=6.489..9.410 rows=2868 loops=1)
                          Rows Removed by Filter: 99228
                    ->  Parallel Seq Scan on medicion_horaria_2023_01 medicion_horaria_1  (cost=0.00..1694.29 rows=1724 width=22) (actual time=6.407..8.574 rows=2936 loops=1)
                          Rows Removed by Filter: 97743
                    ->  Parallel Seq Scan on medicion_horaria_2023_09 medicion_horaria_9  (cost=0.00..1683.71 rows=1626 width=23) (actual time=6.710..8.862 rows=2880 loops=1)
                          Rows Removed by Filter: 96904
                    ->  Parallel Seq Scan on medicion_horaria_2023_03 medicion_horaria_3  (cost=0.00..1667.65 rows=1791 width=23) (actual time=8.655..11.640 rows=2968 loops=1)
                          Rows Removed by Filter: 95857
                    ->  Parallel Seq Scan on medicion_horaria_2023_04 medicion_horaria_4  (cost=0.00..1647.73 rows=1696 width=22) (actual time=7.306..9.844 rows=2880 loops=1)
                          Rows Removed by Filter: 94867
                    ->  Parallel Seq Scan on medicion_horaria_2023_02 medicion_horaria_2  (cost=0.00..1497.36 rows=1526 width=23) (actual time=5.569..7.586 rows=2672 loops=1)
                          Rows Removed by Filter: 86049
Execution Time: 114.289 ms
```

**con_indice_btree** (0,404 ms)

```
  ->  Merge Append  (cost=8.73..67246.85 rows=84524 width=22) (actual time=0.236..0.252 rows=50 loops=1)
        ->  Index Scan using medicion_horaria_2023_01_id_sensor_timestamp_idx on medicion_horaria_2023_01 medicion_horaria_1  (cost=0.29..2539.82 rows=2930 width=22) (actual time=0.019..0.019 rows=1 loops=1)
              Index Cond: ((id_sensor)::text = 'S-032'::text)
        ->  Index Scan using medicion_horaria_2023_02_id_sensor_timestamp_idx on medicion_horaria_2023_02 medicion_horaria_2  (cost=0.29..2264.00 rows=2594 width=23) (actual time=0.011..0.011 rows=1 loops=1)
              Index Cond: ((id_sensor)::text = 'S-032'::text)
        ->  Index Scan using medicion_horaria_2023_03_id_sensor_timestamp_idx on medicion_horaria_2023_03 medicion_horaria_3  (cost=0.29..2521.80 rows=3044 width=23) (actual time=0.007..0.007 rows=1 loops=1)
              Index Cond: ((id_sensor)::text = 'S-032'::text)
        ->  Index Scan using medicion_horaria_2023_04_id_sensor_timestamp_idx on medicion_horaria_2023_04 medicion_horaria_4  (cost=0.29..2461.25 rows=2884 width=22) (actual time=0.009..0.009 rows=1 loops=1)
              Index Cond: ((id_sensor)::text = 'S-032'::text)
        ->  Index Scan using medicion_horaria_2023_05_id_sensor_timestamp_idx on medicion_horaria_2023_05 medicion_horaria_5  (cost=0.29..2578.85 rows=3006 width=22) (actual time=0.009..0.009 rows=1 loops=1)
              Index Cond: ((id_sensor)::text = 'S-032'::text)
        ->  Index Scan using medicion_horaria_2023_06_id_sensor_timestamp_idx on medicion_horaria_2023_06 medicion_horaria_6  (cost=0.29..2530.31 rows=2869 width=22) (actual time=0.009..0.009 rows=1 loops=1)
              Index Cond: ((id_sensor)::text = 'S-032'::text)
        ->  Index Scan using medicion_horaria_2023_07_id_sensor_timestamp_idx on medicion_horaria_2023_07 medicion_horaria_7  (cost=0.29..2624.87 rows=2878 width=23) (actual time=0.010..0.010 rows=1 loops=1)
              Index Cond: ((id_sensor)::text = 'S-032'::text)
        ->  Index Scan using medicion_horaria_2023_08_id_sensor_timestamp_idx on medicion_horaria_2023_08 medicion_horaria_8  (cost=0.29..2625.90 rows=2833 width=23) (actual time=0.009..0.009 rows=1 loops=1)
              Index Cond: ((id_sensor)::text = 'S-032'::text)
        ->  Index Scan using medicion_horaria_2023_09_id_sensor_timestamp_idx on medicion_horaria_2023_09 medicion_horaria_9  (cost=0.29..2504.14 rows=2764 width=23) (actual time=0.010..0.010 rows=1 loops=1)
              Index Cond: ((id_sensor)::text = 'S-032'::text)
        ->  Index Scan using medicion_horaria_2023_10_id_sensor_timestamp_idx on medicion_horaria_2023_10 medicion_horaria_10  (cost=0.29..2606.67 rows=2985 width=23) (actual time=0.009..0.009 rows=1 loops=1)
              Index Cond: ((id_sensor)::text = 'S-032'::text)
        ->  Index Scan using medicion_horaria_2023_11_id_sensor_timestamp_idx on medicion_horaria_2023_11 medicion_horaria_11  (cost=0.29..2591.21 rows=2877 width=22) (actual time=0.009..0.009 rows=1 loops=1)
              Index Cond: ((id_sensor)::text = 'S-032'::text)
        ->  Index Scan using medicion_horaria_2023_12_id_sensor_timestamp_idx on medicion_horaria_2023_12 medicion_horaria_12  (cost=0.29..2653.77 rows=2973 width=22) (actual time=0.010..0.010 rows=1 loops=1)
              Index Cond: ((id_sensor)::text = 'S-032'::text)
        ->  Index Scan using medicion_horaria_2024_01_id_sensor_timestamp_idx on medicion_horaria_2024_01 medicion_horaria_13  (cost=0.29..2659.97 rows=2922 width=22) (actual time=0.010..0.010 rows=1 loops=1)
              Index Cond: ((id_sensor)::text = 'S-032'::text)
        ->  Index Scan using medicion_horaria_2024_02_id_sensor_timestamp_idx on medicion_horaria_2024_02 medicion_horaria_14  (cost=0.29..2568.77 rows=2927 width=22) (actual time=0.010..0.010 rows=1 loops=1)
              Index Cond: ((id_sensor)::text = 'S-032'::text)
        ->  Index Scan using medicion_horaria_2024_03_id_sensor_timestamp_idx on medicion_horaria_2024_03 medicion_horaria_15  (cost=0.42..2994.34 rows=4335 width=22) (actual time=0.010..0.010 rows=1 loops=1)
              Index Cond: ((id_sensor)::text = 'S-032'::text)
        ->  Index Scan using medicion_horaria_2024_04_id_sensor_timestamp_idx on medicion_horaria_2024_04 medicion_horaria_16  (cost=0.42..2913.32 rows=4233 width=22) (actual time=0.006..0.006 rows=1 loops=1)
              Index Cond: ((id_sensor)::text = 'S-032'::text)
        ->  Index Scan using medicion_horaria_2024_05_id_sensor_timestamp_idx on medicion_horaria_2024_05 medicion_horaria_17  (cost=0.42..3013.54 rows=4493 width=22) (actual time=0.012..0.012 rows=1 loops=1)
              Index Cond: ((id_sensor)::text = 'S-032'::text)
        ->  Index Scan using medicion_horaria_2024_06_id_sensor_timestamp_idx on medicion_horaria_2024_06 medicion_horaria_18  (cost=0.42..2880.43 rows=4399 width=22) (actual time=0.010..0.010 rows=1 loops=1)
              Index Cond: ((id_sensor)::text = 'S-032'::text)
        ->  Index Scan using medicion_horaria_2024_07_id_sensor_timestamp_idx on medicion_horaria_2024_07 medicion_horaria_19  (cost=0.42..2953.90 rows=4355 width=22) (actual time=0.009..0.009 rows=1 loops=1)
              Index Cond: ((id_sensor)::text = 'S-032'::text)
        ->  Index Scan using medicion_horaria_2024_08_id_sensor_timestamp_idx on medicion_horaria_2024_08 medicion_horaria_20  (cost=0.42..2933.42 rows=4645 width=22) (actual time=0.012..0.012 rows=1 loops=1)
              Index Cond: ((id_sensor)::text = 'S-032'::text)
        ->  Index Scan using medicion_horaria_2024_09_id_sensor_timestamp_idx on medicion_horaria_2024_09 medicion_horaria_21  (cost=0.29..2824.83 rows=4317 width=22) (actual time=0.009..0.009 rows=1 loops=1)
              Index Cond: ((id_sensor)::text = 'S-032'::text)
        ->  Index Scan using medicion_horaria_2024_10_id_sensor_timestamp_idx on medicion_horaria_2024_10 medicion_horaria_22  (cost=0.42..2912.88 rows=4613 width=22) (actual time=0.009..0.009 rows=1 loops=1)
              Index Cond: ((id_sensor)::text = 'S-032'::text)
        ->  Index Scan using medicion_horaria_2024_11_id_sensor_timestamp_idx on medicion_horaria_2024_11 medicion_horaria_23  (cost=0.29..2793.89 rows=4249 width=22) (actual time=0.009..0.009 rows=1 loops=1)
              Index Cond: ((id_sensor)::text = 'S-032'::text)
        ->  Index Scan using medicion_horaria_2024_12_id_sensor_timestamp_idx on medicion_horaria_2024_12 medicion_horaria_24  (cost=0.42..2901.04 rows=4398 width=22) (actual time=0.007..0.019 rows=50 loops=1)
              Index Cond: ((id_sensor)::text = 'S-032'::text)
        ->  Index Scan using medicion_horaria_default_id_sensor_timestamp_idx on medicion_horaria_default medicion_horaria_25  (cost=0.12..8.14 rows=1 width=114) (actual time=0.001..0.001 rows=0 loops=1)
              Index Cond: ((id_sensor)::text = 'S-032'::text)
Execution Time: 0.310 ms
```

## A2.5-2 — Promedio por variable de un mes

```sql
SELECT variable, AVG(valor) FROM medicion_horaria WHERE timestamp >= '2024-12-01'::timestamptz AND timestamp < '2024-12-01'::timestamptz + interval '1 month' GROUP BY variable
```

**sin_poda_expresion** (254,525 ms)

```
                    ->  Parallel Append  (cost=0.00..52060.68 rows=5482 width=14) (actual time=169.663..204.289 rows=39483 loops=3)
                          ->  Parallel Seq Scan on medicion_horaria_2024_05 medicion_horaria_17  (cost=0.00..2459.32 rows=366 width=14) (actual time=26.820..26.821 rows=0 loops=1)
                                Rows Removed by Filter: 124471
                          ->  Parallel Seq Scan on medicion_horaria_2024_03 medicion_horaria_15  (cost=0.00..2442.46 rows=364 width=14) (actual time=25.759..25.759 rows=0 loops=1)
                                Rows Removed by Filter: 123610
                          ->  Parallel Seq Scan on medicion_horaria_2024_07 medicion_horaria_19  (cost=0.00..2430.48 rows=362 width=14) (actual time=25.081..25.081 rows=0 loops=1)
                                Rows Removed by Filter: 123127
                          ->  Parallel Seq Scan on medicion_horaria_2024_04 medicion_horaria_16  (cost=0.00..2397.88 rows=357 width=14) (actual time=25.382..25.382 rows=0 loops=1)
                                Rows Removed by Filter: 121417
                          ->  Parallel Seq Scan on medicion_horaria_2024_10 medicion_horaria_22  (cost=0.00..2378.96 rows=355 width=14) (actual time=26.375..26.376 rows=0 loops=1)
                                Rows Removed by Filter: 120648
                          ->  Parallel Seq Scan on medicion_horaria_2024_08 medicion_horaria_20  (cost=0.00..2368.59 rows=353 width=14) (actual time=24.981..24.981 rows=0 loops=1)
                                Rows Removed by Filter: 120029
                          ->  Parallel Seq Scan on medicion_horaria_2024_06 medicion_horaria_18  (cost=0.00..2361.47 rows=352 width=14) (actual time=24.652..24.652 rows=0 loops=1)
                                Rows Removed by Filter: 119531
                          ->  Parallel Seq Scan on medicion_horaria_2024_12 medicion_horaria_24  (cost=0.00..2338.33 rows=348 width=14) (actual time=0.054..41.239 rows=118449 loops=1)
                          ->  Parallel Seq Scan on medicion_horaria_2024_09 medicion_horaria_21  (cost=0.00..2291.63 rows=342 width=14) (actual time=26.482..26.482 rows=0 loops=1)
                                Rows Removed by Filter: 116147
                          ->  Parallel Seq Scan on medicion_horaria_2024_11 medicion_horaria_23  (cost=0.00..2269.29 rows=338 width=14) (actual time=26.137..26.137 rows=0 loops=1)
                                Rows Removed by Filter: 115045
                          ->  Parallel Seq Scan on medicion_horaria_2024_01 medicion_horaria_13  (cost=0.00..2128.32 rows=316 width=14) (actual time=30.421..30.422 rows=0 loops=1)
                                Rows Removed by Filter: 107568
                          ->  Parallel Seq Scan on medicion_horaria_2023_12 medicion_horaria_12  (cost=0.00..2115.79 rows=315 width=14) (actual time=20.990..20.990 rows=0 loops=1)
                                Rows Removed by Filter: 106934
                          ->  Parallel Seq Scan on medicion_horaria_2023_07 medicion_horaria_7  (cost=0.00..2101.18 rows=312 width=15) (actual time=20.885..20.885 rows=0 loops=1)
                                Rows Removed by Filter: 106195
                          ->  Parallel Seq Scan on medicion_horaria_2023_08 medicion_horaria_8  (cost=0.00..2092.35 rows=311 width=15) (actual time=9.700..9.700 rows=0 loops=3)
                                Rows Removed by Filter: 35242
                          ->  Parallel Seq Scan on medicion_horaria_2023_11 medicion_horaria_11  (cost=0.00..2065.45 rows=307 width=14) (actual time=11.902..11.902 rows=0 loops=2)
                                Rows Removed by Filter: 52188
                          ->  Parallel Seq Scan on medicion_horaria_2024_02 medicion_horaria_14  (cost=0.00..2060.19 rows=306 width=14) (actual time=28.729..28.729 rows=0 loops=1)
                                Rows Removed by Filter: 104156
                          ->  Parallel Seq Scan on medicion_horaria_2023_10 medicion_horaria_10  (cost=0.00..2051.75 rows=305 width=15) (actual time=27.992..27.992 rows=0 loops=1)
                                Rows Removed by Filter: 103530
                          ->  Parallel Seq Scan on medicion_horaria_2023_05 medicion_horaria_5  (cost=0.00..2042.41 rows=304 width=14) (actual time=27.299..27.300 rows=0 loops=1)
                                Rows Removed by Filter: 103303
                          ->  Parallel Seq Scan on medicion_horaria_2023_06 medicion_horaria_6  (cost=0.00..2017.99 rows=300 width=14) (actual time=26.707..26.707 rows=0 loops=1)
                                Rows Removed by Filter: 102096
                          ->  Parallel Seq Scan on medicion_horaria_2023_01 medicion_horaria_1  (cost=0.00..1990.40 rows=296 width=14) (actual time=20.306..20.306 rows=0 loops=1)
                                Rows Removed by Filter: 100679
                          ->  Parallel Seq Scan on medicion_horaria_2023_09 medicion_horaria_9  (cost=0.00..1977.19 rows=294 width=15) (actual time=22.248..22.248 rows=0 loops=1)
                                Rows Removed by Filter: 99784
                          ->  Parallel Seq Scan on medicion_horaria_2023_03 medicion_horaria_3  (cost=0.00..1958.32 rows=291 width=15) (actual time=18.222..18.222 rows=0 loops=1)
                                Rows Removed by Filter: 98825
                          ->  Parallel Seq Scan on medicion_horaria_2023_04 medicion_horaria_4  (cost=0.00..1935.22 rows=288 width=14) (actual time=17.968..17.968 rows=0 loops=1)
                                Rows Removed by Filter: 97747
                          ->  Parallel Seq Scan on medicion_horaria_2023_02 medicion_horaria_2  (cost=0.00..1758.30 rows=261 width=15) (actual time=17.060..17.060 rows=0 loops=1)
                                Rows Removed by Filter: 88721
                          ->  Parallel Seq Scan on medicion_horaria_default medicion_horaria_25  (cost=0.00..0.00 rows=1 width=106) (actual time=0.001..0.001 rows=0 loops=1)
Execution Time: 256.776 ms
```

**con_poda_particiones** (24,838 ms)

```
                    ->  Parallel Append  (cost=0.00..2585.05 rows=49345 width=14) (actual time=0.007..14.122 rows=39483 loops=3)
                          Subplans Removed: 1
                          ->  Parallel Seq Scan on medicion_horaria_2024_12 medicion_horaria_1  (cost=0.00..2338.33 rows=69662 width=14) (actual time=0.006..11.481 rows=39483 loops=3)
Execution Time: 39.291 ms
```

## A2.5-3 — Conteo y promedio de 3 meses

```sql
SELECT COUNT(*), AVG(valor) FROM medicion_horaria WHERE timestamp >= '2024-12-01'::timestamptz - interval '3 months' AND timestamp < '2024-12-01'::timestamptz
```

**btree_timestamp** (41,796 ms)

```
              ->  Parallel Append  (cost=1590.80..12666.60 rows=146622 width=8) (actual time=3.351..25.786 rows=117280 loops=3)
                    Subplans Removed: 21
                    ->  Parallel Bitmap Heap Scan on medicion_horaria_2024_10 medicion_horaria_2  (cost=1656.94..4035.90 rows=70969 width=8) (actual time=3.288..15.500 rows=120648 loops=1)
                          Recheck Cond: (("timestamp" >= ('2024-12-01 00:00:00+00'::timestamp with time zone - '3 mons'::interval)) AND ("timestamp" < '2024-12-01 00:00:00+00'::timestamp with time zone))
                          ->  Bitmap Index Scan on medicion_horaria_2024_10_timestamp_idx1  (cost=0.00..1626.78 rows=120648 width=0) (actual time=3.164..3.164 rows=120648 loops=1)
                                Index Cond: (("timestamp" >= ('2024-12-01 00:00:00+00'::timestamp with time zone - '3 mons'::interval)) AND ("timestamp" < '2024-12-01 00:00:00+00'::timestamp with time zone))
                    ->  Parallel Bitmap Heap Scan on medicion_horaria_2024_09 medicion_horaria_1  (cost=1590.80..3882.43 rows=68322 width=8) (actual time=1.094..6.695 rows=38716 loops=3)
                          Recheck Cond: (("timestamp" >= ('2024-12-01 00:00:00+00'::timestamp with time zone - '3 mons'::interval)) AND ("timestamp" < '2024-12-01 00:00:00+00'::timestamp with time zone))
                          Heap Blocks: exact=98
                          ->  Bitmap Index Scan on medicion_horaria_2024_09_timestamp_idx1  (cost=0.00..1561.77 rows=116147 width=0) (actual time=3.099..3.099 rows=116147 loops=1)
                                Index Cond: (("timestamp" >= ('2024-12-01 00:00:00+00'::timestamp with time zone - '3 mons'::interval)) AND ("timestamp" < '2024-12-01 00:00:00+00'::timestamp with time zone))
                    ->  Parallel Bitmap Heap Scan on medicion_horaria_2024_11 medicion_horaria_3  (cost=1571.51..3840.79 rows=67674 width=8) (actual time=3.535..18.351 rows=115045 loops=1)
                          Recheck Cond: (("timestamp" >= ('2024-12-01 00:00:00+00'::timestamp with time zone - '3 mons'::interval)) AND ("timestamp" < '2024-12-01 00:00:00+00'::timestamp with time zone))
                          Heap Blocks: exact=1085
                          ->  Bitmap Index Scan on medicion_horaria_2024_11_timestamp_idx1  (cost=0.00..1542.75 rows=115045 width=0) (actual time=3.271..3.271 rows=115045 loops=1)
                                Index Cond: (("timestamp" >= ('2024-12-01 00:00:00+00'::timestamp with time zone - '3 mons'::interval)) AND ("timestamp" < '2024-12-01 00:00:00+00'::timestamp with time zone))
Execution Time: 43.567 ms
```

**brin_timestamp** (50,980 ms)

```
              ->  Parallel Append  (cost=41.32..50806.30 rows=146593 width=8) (actual time=0.267..37.167 rows=117280 loops=3)
                    Subplans Removed: 21
                    ->  Parallel Bitmap Heap Scan on medicion_horaria_2024_10 medicion_horaria_2  (cost=42.45..2421.41 rows=70955 width=8) (actual time=0.068..10.761 rows=40216 loops=3)
                          Recheck Cond: (("timestamp" >= ('2024-12-01 00:00:00+00'::timestamp with time zone - '3 mons'::interval)) AND ("timestamp" < '2024-12-01 00:00:00+00'::timestamp with time zone))
                          Heap Blocks: lossy=47
                          ->  Bitmap Index Scan on medicion_horaria_2024_10_timestamp_idx  (cost=0.00..12.29 rows=120648 width=0) (actual time=0.123..0.123 rows=11370 loops=1)
                                Index Cond: (("timestamp" >= ('2024-12-01 00:00:00+00'::timestamp with time zone - '3 mons'::interval)) AND ("timestamp" < '2024-12-01 00:00:00+00'::timestamp with time zone))
                    ->  Parallel Bitmap Heap Scan on medicion_horaria_2024_09 medicion_horaria_1  (cost=41.32..2332.95 rows=68308 width=8) (actual time=0.079..15.733 rows=58074 loops=2)
                          Recheck Cond: (("timestamp" >= ('2024-12-01 00:00:00+00'::timestamp with time zone - '3 mons'::interval)) AND ("timestamp" < '2024-12-01 00:00:00+00'::timestamp with time zone))
                          Heap Blocks: lossy=153
                          ->  Bitmap Index Scan on medicion_horaria_2024_09_timestamp_idx  (cost=0.00..12.29 rows=116147 width=0) (actual time=0.129..0.129 rows=10960 loops=1)
                                Index Cond: (("timestamp" >= ('2024-12-01 00:00:00+00'::timestamp with time zone - '3 mons'::interval)) AND ("timestamp" < '2024-12-01 00:00:00+00'::timestamp with time zone))
                    ->  Parallel Bitmap Heap Scan on medicion_horaria_2024_11 medicion_horaria_3  (cost=41.05..2310.33 rows=67661 width=8) (actual time=0.514..25.370 rows=115045 loops=1)
                          Recheck Cond: (("timestamp" >= ('2024-12-01 00:00:00+00'::timestamp with time zone - '3 mons'::interval)) AND ("timestamp" < '2024-12-01 00:00:00+00'::timestamp with time zone))
                          Heap Blocks: lossy=1085
                          ->  Bitmap Index Scan on medicion_horaria_2024_11_timestamp_idx  (cost=0.00..12.29 rows=115045 width=0) (actual time=0.503..0.503 rows=10850 loops=1)
                                Index Cond: (("timestamp" >= ('2024-12-01 00:00:00+00'::timestamp with time zone - '3 mons'::interval)) AND ("timestamp" < '2024-12-01 00:00:00+00'::timestamp with time zone))
Execution Time: 53.727 ms
```

**plan_elegido_por_postgres** (54,171 ms)

```
              ->  Parallel Append  (cost=0.29..42573.10 rows=146593 width=8) (actual time=0.010..36.792 rows=117280 loops=3)
                    Subplans Removed: 21
                    ->  Parallel Seq Scan on medicion_horaria_2024_10 medicion_horaria_2  (cost=0.00..2378.96 rows=70955 width=8) (actual time=0.008..9.755 rows=40216 loops=3)
                    ->  Parallel Seq Scan on medicion_horaria_2024_09 medicion_horaria_1  (cost=0.00..2291.63 rows=68308 width=8) (actual time=0.012..25.605 rows=116147 loops=1)
                    ->  Parallel Seq Scan on medicion_horaria_2024_11 medicion_horaria_3  (cost=0.00..2269.29 rows=67661 width=8) (actual time=0.009..15.811 rows=57522 loops=2)
Execution Time: 53.238 ms
```

## A2.5-4 — Mediciones por zona y día

```sql
SELECT zona, fecha, mediciones FROM mv_kpi_diario_zona
```

**consulta_en_vivo** (1.854,426 ms)

```
                    ->  Index Scan using ubicaciones_nombre_zona_key on ubicaciones u  (cost=0.14..50.55 rows=160 width=222) (actual time=0.016..0.041 rows=5 loops=1)
                    ->  Index Scan using idx_sensores_ubicacion on sensores s  (cost=0.14..0.42 rows=10 width=10) (actual time=0.011..0.057 rows=10 loops=5)
                          Index Cond: (id_ubicacion = u.id_ubicacion)
              ->  Append  (cost=0.29..1701.60 rows=56960 width=14) (actual time=0.022..8.644 rows=52642 loops=50)
                    ->  Index Only Scan using medicion_horaria_2023_01_id_sensor_timestamp_idx on medicion_horaria_2023_01 m_1  (cost=0.29..56.56 rows=2237 width=14) (actual time=0.012..0.215 rows=2014 loops=50)
                          Index Cond: (id_sensor = (s.id_sensor)::text)
                    ->  Index Only Scan using medicion_horaria_2023_02_id_sensor_timestamp_idx on medicion_horaria_2023_02 m_2  (cost=0.29..49.84 rows=1972 width=14) (actual time=0.012..0.191 rows=1774 loops=50)
                          Index Cond: (id_sensor = (s.id_sensor)::text)
                    ->  Index Only Scan using medicion_horaria_2023_03_id_sensor_timestamp_idx on medicion_horaria_2023_03 m_3  (cost=0.29..54.16 rows=2196 width=14) (actual time=0.011..0.204 rows=1976 loops=50)
                          Index Cond: (id_sensor = (s.id_sensor)::text)
                    ->  Index Only Scan using medicion_horaria_2023_04_id_sensor_timestamp_idx on medicion_horaria_2023_04 m_4  (cost=0.29..53.66 rows=2172 width=14) (actual time=0.012..0.205 rows=1955 loops=50)
                          Index Cond: (id_sensor = (s.id_sensor)::text)
                    ->  Index Only Scan using medicion_horaria_2023_05_id_sensor_timestamp_idx on medicion_horaria_2023_05 m_5  (cost=0.29..57.51 rows=2296 width=14) (actual time=0.010..0.214 rows=2066 loops=50)
                          Index Cond: (id_sensor = (s.id_sensor)::text)
                    ->  Index Only Scan using medicion_horaria_2023_06_id_sensor_timestamp_idx on medicion_horaria_2023_06 m_6  (cost=0.29..54.73 rows=2219 width=14) (actual time=0.010..0.209 rows=2042 loops=50)
                          Index Cond: (id_sensor = (s.id_sensor)::text)
                    ->  Index Only Scan using medicion_horaria_2023_07_id_sensor_timestamp_idx on medicion_horaria_2023_07 m_7  (cost=0.29..58.14 rows=2309 width=14) (actual time=0.011..0.220 rows=2124 loops=50)
                          Index Cond: (id_sensor = (s.id_sensor)::text)
                    ->  Index Only Scan using medicion_horaria_2023_08_id_sensor_timestamp_idx on medicion_horaria_2023_08 m_8  (cost=0.29..57.95 rows=2298 width=14) (actual time=0.011..0.220 rows=2114 loops=50)
                          Index Cond: (id_sensor = (s.id_sensor)::text)
                    ->  Index Only Scan using medicion_horaria_2023_09_id_sensor_timestamp_idx on medicion_horaria_2023_09 m_9  (cost=0.29..54.53 rows=2217 width=14) (actual time=0.012..0.214 rows=1996 loops=50)
                          Index Cond: (id_sensor = (s.id_sensor)::text)
                    ->  Index Only Scan using medicion_horaria_2023_10_id_sensor_timestamp_idx on medicion_horaria_2023_10 m_10  (cost=0.29..57.84 rows=2301 width=14) (actual time=0.011..0.218 rows=2071 loops=50)
                          Index Cond: (id_sensor = (s.id_sensor)::text)
                    ->  Index Only Scan using medicion_horaria_2023_11_id_sensor_timestamp_idx on medicion_horaria_2023_11 m_11  (cost=0.29..58.15 rows=2319 width=14) (actual time=0.011..0.214 rows=2088 loops=50)
                          Index Cond: (id_sensor = (s.id_sensor)::text)
                    ->  Index Only Scan using medicion_horaria_2023_12_id_sensor_timestamp_idx on medicion_horaria_2023_12 m_12  (cost=0.29..58.42 rows=2325 width=14) (actual time=0.011..0.221 rows=2139 loops=50)
                          Index Cond: (id_sensor = (s.id_sensor)::text)
                    ->  Index Only Scan using medicion_horaria_2024_01_id_sensor_timestamp_idx on medicion_horaria_2024_01 m_13  (cost=0.29..57.87 rows=2289 width=14) (actual time=0.012..0.222 rows=2151 loops=50)
                          Index Cond: (id_sensor = (s.id_sensor)::text)
                    ->  Index Only Scan using medicion_horaria_2024_02_id_sensor_timestamp_idx on medicion_horaria_2024_02 m_14  (cost=0.29..55.87 rows=2170 width=14) (actual time=0.011..0.212 rows=2083 loops=50)
                          Index Cond: (id_sensor = (s.id_sensor)::text)
                    ->  Index Only Scan using medicion_horaria_2024_03_id_sensor_timestamp_idx on medicion_horaria_2024_03 m_15  (cost=0.42..63.56 rows=2575 width=14) (actual time=0.016..0.252 rows=2472 loops=50)
                          Index Cond: (id_sensor = (s.id_sensor)::text)
                    ->  Index Only Scan using medicion_horaria_2024_04_id_sensor_timestamp_idx on medicion_horaria_2024_04 m_16  (cost=0.42..63.46 rows=2583 width=14) (actual time=0.012..0.243 rows=2428 loops=50)
                          Index Cond: (id_sensor = (s.id_sensor)::text)
                    ->  Index Only Scan using medicion_horaria_2024_05_id_sensor_timestamp_idx on medicion_horaria_2024_05 m_17  (cost=0.42..63.87 rows=2593 width=14) (actual time=0.013..0.252 rows=2489 loops=50)
                          Index Cond: (id_sensor = (s.id_sensor)::text)
                    ->  Index Only Scan using medicion_horaria_2024_06_id_sensor_timestamp_idx on medicion_horaria_2024_06 m_18  (cost=0.42..61.91 rows=2490 width=14) (actual time=0.014..0.243 rows=2391 loops=50)
                          Index Cond: (id_sensor = (s.id_sensor)::text)
                    ->  Index Only Scan using medicion_horaria_2024_07_id_sensor_timestamp_idx on medicion_horaria_2024_07 m_19  (cost=0.42..64.27 rows=2620 width=14) (actual time=0.013..0.250 rows=2463 loops=50)
                          Index Cond: (id_sensor = (s.id_sensor)::text)
                    ->  Index Only Scan using medicion_horaria_2024_08_id_sensor_timestamp_idx on medicion_horaria_2024_08 m_20  (cost=0.42..63.11 rows=2554 width=14) (actual time=0.013..0.246 rows=2401 loops=50)
                          Index Cond: (id_sensor = (s.id_sensor)::text)
                    ->  Index Only Scan using medicion_horaria_2024_09_id_sensor_timestamp_idx on medicion_horaria_2024_09 m_21  (cost=0.29..62.16 rows=2525 width=14) (actual time=0.012..0.239 rows=2323 loops=50)
                          Index Cond: (id_sensor = (s.id_sensor)::text)
                    ->  Index Only Scan using medicion_horaria_2024_10_id_sensor_timestamp_idx on medicion_horaria_2024_10 m_22  (cost=0.42..64.24 rows=2623 width=14) (actual time=0.013..0.244 rows=2413 loops=50)
                          Index Cond: (id_sensor = (s.id_sensor)::text)
                    ->  Index Only Scan using medicion_horaria_2024_11_id_sensor_timestamp_idx on medicion_horaria_2024_11 m_23  (cost=0.29..61.66 rows=2501 width=14) (actual time=0.012..0.232 rows=2301 loops=50)
                          Index Cond: (id_sensor = (s.id_sensor)::text)
                    ->  Index Only Scan using medicion_horaria_2024_12_id_sensor_timestamp_idx on medicion_horaria_2024_12 m_24  (cost=0.42..63.32 rows=2575 width=14) (actual time=0.013..0.237 rows=2369 loops=50)
                          Index Cond: (id_sensor = (s.id_sensor)::text)
                    ->  Seq Scan on medicion_horaria_default m_25  (cost=0.00..0.00 rows=1 width=66) (actual time=0.001..0.001 rows=0 loops=50)
Execution Time: 1912.229 ms
```

**vista_materializada** (0,472 ms)

```
Seq Scan on mv_kpi_diario_zona  (cost=0.00..94.55 rows=3655 width=22) (actual time=0.008..0.345 rows=3655 loops=1)
Execution Time: 0.462 ms
```
