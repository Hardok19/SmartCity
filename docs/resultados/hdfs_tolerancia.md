# Tolerancia a fallos de HDFS (A3.4)

Generado por `hadoop/hdfs/04_tolerancia.ps1` el 2026-11-27 07:40 (hora local).
Clúster: 1 NameNode + 3 DataNodes, `dfs.replication = 2`. Archivo: `/smartcity/sandbox/tolerancia/trafico_2024.csv` (2.66 MB, bloques de 1 MB).
Nodo apagado: **smartcity_datanode2**.

| Paso | Resultado |
|---|---|
| DataNodes vivos al inicio | 3 |
| Lectura completa con el nodo apagado | **OK**: md5 idéntico al original, 1.8 s (0 avisos del cliente al intentar el nodo caído y pasar a otra réplica) |
| El NameNode declara muerto el DataNode | 97 s después de apagarlo (2 x recheck 30 s + 10 x latido 3 s = 90 s teóricos) |
| Todos los bloques vuelven a tener 2 réplicas vivas | 152 s después de apagarlo |
| El DataNode vuelve a reportarse al encenderlo | 7 s |

## Qué muestra

1. **Disponibilidad:** cada bloque tiene 2 réplicas en nodos distintos. Al caer un nodo, el cliente lee la otra réplica sin error: la lectura no se interrumpe.
2. **Detección:** el NameNode no se entera al instante. Marca el nodo como muerto cuando deja de recibir sus latidos durante el intervalo configurado.
3. **Auto-reparación:** los bloques que quedaron con 1 réplica (*under-replicated*) se copian a otro DataNode vivo hasta volver al factor 2, sin intervención manual.
4. **Reingreso:** al volver el nodo, sus réplicas sobran (*over-replicated*) y el NameNode elimina las excedentes.

## fsck antes de apagar el nodo
```
Connecting to namenode via http://namenode:9870/fsck?ugi=root&files=1&blocks=1&locations=1&path=%2Fsmartcity%2Fsandbox%2Ftolerancia%2Ftrafico_2024.csv
0. BP-1604020310-172.18.0.2-1790327252813:blk_1073741981_1157 len=1048576 Live_repl=2  [DatanodeInfoWithStorage[smartcity_datanode1,DS-4af8c4c4-f71d-480c-abfa-a02c4766d648,DISK], DatanodeInfoWithStorage[smartcity_datanode3,DS-0bd9bb4b-f4af-48ca-b028-a3725aa4fa76,DISK]]
1. BP-1604020310-172.18.0.2-1790327252813:blk_1073741982_1158 len=1048576 Live_repl=2  [DatanodeInfoWithStorage[smartcity_datanode1,DS-4af8c4c4-f71d-480c-abfa-a02c4766d648,DISK], DatanodeInfoWithStorage[smartcity_datanode2,DS-0fc83652-2e43-47a1-96b4-e0cc90336f14,DISK]]
2. BP-1604020310-172.18.0.2-1790327252813:blk_1073741983_1159 len=690146 Live_repl=2  [DatanodeInfoWithStorage[smartcity_datanode3,DS-0bd9bb4b-f4af-48ca-b028-a3725aa4fa76,DISK], DatanodeInfoWithStorage[smartcity_datanode1,DS-4af8c4c4-f71d-480c-abfa-a02c4766d648,DISK]]
Status: HEALTHY
 Under-replicated blocks:	0 (0.0 %)
 Average block replication:	2.0
 Missing blocks:		0
 Missing replicas:		0 (0.0 %)
 Missing block groups:		0
 Missing internal blocks:	0
```

## fsck con el nodo apagado (tras ser declarado muerto)
```
Connecting to namenode via http://namenode:9870/fsck?ugi=root&files=1&blocks=1&locations=1&path=%2Fsmartcity%2Fsandbox%2Ftolerancia%2Ftrafico_2024.csv
0. BP-1604020310-172.18.0.2-1790327252813:blk_1073741981_1157 len=1048576 Live_repl=2  [DatanodeInfoWithStorage[smartcity_datanode1,DS-4af8c4c4-f71d-480c-abfa-a02c4766d648,DISK], DatanodeInfoWithStorage[smartcity_datanode3,DS-0bd9bb4b-f4af-48ca-b028-a3725aa4fa76,DISK]]
1. BP-1604020310-172.18.0.2-1790327252813:blk_1073741982_1158 len=1048576 Live_repl=2  [DatanodeInfoWithStorage[smartcity_datanode1,DS-4af8c4c4-f71d-480c-abfa-a02c4766d648,DISK], DatanodeInfoWithStorage[smartcity_datanode2,DS-0fc83652-2e43-47a1-96b4-e0cc90336f14,DISK]]
2. BP-1604020310-172.18.0.2-1790327252813:blk_1073741983_1159 len=690146 Live_repl=2  [DatanodeInfoWithStorage[smartcity_datanode3,DS-0bd9bb4b-f4af-48ca-b028-a3725aa4fa76,DISK], DatanodeInfoWithStorage[smartcity_datanode1,DS-4af8c4c4-f71d-480c-abfa-a02c4766d648,DISK]]
Status: HEALTHY
 Under-replicated blocks:	0 (0.0 %)
 Average block replication:	2.0
 Missing blocks:		0
 Missing replicas:		0 (0.0 %)
 Missing block groups:		0
 Missing internal blocks:	0
```

## fsck después de la re-replicación
```
Connecting to namenode via http://namenode:9870/fsck?ugi=root&files=1&blocks=1&locations=1&path=%2Fsmartcity%2Fsandbox%2Ftolerancia%2Ftrafico_2024.csv
0. BP-1604020310-172.18.0.2-1790327252813:blk_1073741981_1157 len=1048576 Live_repl=2  [DatanodeInfoWithStorage[smartcity_datanode1,DS-4af8c4c4-f71d-480c-abfa-a02c4766d648,DISK], DatanodeInfoWithStorage[smartcity_datanode3,DS-0bd9bb4b-f4af-48ca-b028-a3725aa4fa76,DISK]]
1. BP-1604020310-172.18.0.2-1790327252813:blk_1073741982_1158 len=1048576 Live_repl=2  [DatanodeInfoWithStorage[smartcity_datanode1,DS-4af8c4c4-f71d-480c-abfa-a02c4766d648,DISK], DatanodeInfoWithStorage[smartcity_datanode3,DS-0bd9bb4b-f4af-48ca-b028-a3725aa4fa76,DISK]]
2. BP-1604020310-172.18.0.2-1790327252813:blk_1073741983_1159 len=690146 Live_repl=2  [DatanodeInfoWithStorage[smartcity_datanode3,DS-0bd9bb4b-f4af-48ca-b028-a3725aa4fa76,DISK], DatanodeInfoWithStorage[smartcity_datanode1,DS-4af8c4c4-f71d-480c-abfa-a02c4766d648,DISK]]
Status: HEALTHY
 Under-replicated blocks:	0 (0.0 %)
 Average block replication:	2.0
 Missing blocks:		0
 Missing replicas:		0 (0.0 %)
 Missing block groups:		0
 Missing internal blocks:	0
```

## fsck con el nodo de nuevo encendido
```
Connecting to namenode via http://namenode:9870/fsck?ugi=root&files=1&blocks=1&locations=1&path=%2Fsmartcity%2Fsandbox%2Ftolerancia%2Ftrafico_2024.csv
0. BP-1604020310-172.18.0.2-1790327252813:blk_1073741981_1157 len=1048576 Live_repl=2  [DatanodeInfoWithStorage[smartcity_datanode1,DS-4af8c4c4-f71d-480c-abfa-a02c4766d648,DISK], DatanodeInfoWithStorage[smartcity_datanode3,DS-0bd9bb4b-f4af-48ca-b028-a3725aa4fa76,DISK]]
1. BP-1604020310-172.18.0.2-1790327252813:blk_1073741982_1158 len=1048576 Live_repl=2  [DatanodeInfoWithStorage[smartcity_datanode1,DS-4af8c4c4-f71d-480c-abfa-a02c4766d648,DISK], DatanodeInfoWithStorage[smartcity_datanode3,DS-0bd9bb4b-f4af-48ca-b028-a3725aa4fa76,DISK]]
2. BP-1604020310-172.18.0.2-1790327252813:blk_1073741983_1159 len=690146 Live_repl=2  [DatanodeInfoWithStorage[smartcity_datanode3,DS-0bd9bb4b-f4af-48ca-b028-a3725aa4fa76,DISK], DatanodeInfoWithStorage[smartcity_datanode1,DS-4af8c4c4-f71d-480c-abfa-a02c4766d648,DISK]]
Status: HEALTHY
 Under-replicated blocks:	0 (0.0 %)
 Average block replication:	2.0
 Missing blocks:		0
 Missing replicas:		0 (0.0 %)
 Missing block groups:		0
 Missing internal blocks:	0
```
