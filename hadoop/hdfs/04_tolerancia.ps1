<#
.SYNOPSIS
  A3.4 — Tolerancia a fallos de HDFS: se apaga un DataNode y el archivo sigue disponible; HDFS detecta la
  caída y re-replica los bloques en otro nodo. Resultado: docs/resultados/hdfs_tolerancia.md

.EXAMPLE
  .\hadoop\hdfs\04_tolerancia.ps1                       # usa data/raw/trafico/trafico_2024.csv con bloques de 1 MB
  .\hadoop\hdfs\04_tolerancia.ps1 -Nodo smartcity_datanode3

.NOTES
  Con hadoop.env: dfs.heartbeat.interval = 3 s y heartbeat.recheck-interval = 30 s, el NameNode declara
  muerto un DataNode tras 2 x 30 + 10 x 3 = 90 s sin latidos (con los valores por defecto serían 10,5 min).
#>
param(
    [string]$Nodo = "smartcity_datanode2",
    [string]$Archivo = "data/raw/trafico/trafico_2024.csv",
    [int]$Bloque = 1048576
)
$ErrorActionPreference = "Continue"
$NN = "smartcity_namenode"
$Ruta = "/smartcity/sandbox/tolerancia/" + (Split-Path $Archivo -Leaf)

function En-NameNode([string]$cmd) { (docker exec $NN bash -c "$cmd 2>&1" 2>&1 | ForEach-Object { "$_" }) -join "`n" }
function Vivos { [int]((En-NameNode "hdfs dfsadmin -report | grep -oP 'Live datanodes \(\K[0-9]+'").Trim()) }
function Fsck { En-NameNode "hdfs fsck $Ruta -files -blocks -locations | grep -E 'blk_|Under-replicated|Status|Average block replication|Missing'" }
function Md5Hdfs {
    # stderr aparte: el cliente avisa (WARN) que no pudo conectar con el nodo caído y prueba la otra réplica
    $s = (docker exec $NN bash -c "hdfs dfs -cat $Ruta 2>/tmp/cat_err | md5sum; grep -c 'WARN' /tmp/cat_err" 2>$null) -split "`n"
    $script:avisosLectura = [int]($s[1])
    $s[0].Split(" ")[0]
}
function Ip([string]$c) { (docker inspect -f "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}" $c).Trim() }

$ipNodo = Ip $Nodo
$mapa = @{}
foreach ($c in "smartcity_datanode1", "smartcity_datanode2", "smartcity_datanode3") { $mapa[(Ip $c)] = $c }
function Nombres([string]$texto) { foreach ($k in $mapa.Keys) { $texto = $texto -replace [regex]::Escape("$k`:9866"), $mapa[$k] }; $texto }

Write-Host "1) Preparando $Ruta (bloques de $($Bloque/1MB) MB, replicación 2)..."
docker cp $Archivo "${NN}:/tmp/tolerancia.dat" | Out-Null
En-NameNode "hdfs dfs -mkdir -p /smartcity/sandbox/tolerancia && hdfs dfs -D dfs.blocksize=$Bloque -put -f /tmp/tolerancia.dat $Ruta && rm /tmp/tolerancia.dat" | Out-Null
$md5Original = (Get-FileHash -Algorithm MD5 $Archivo).Hash.ToLower()
$fsckInicial = Nombres (Fsck)
$vivosInicial = Vivos

Write-Host "2) Apagando $Nodo ($ipNodo)..."
$t0 = Get-Date
docker stop $Nodo | Out-Null

Write-Host "3) Leyendo el archivo completo con el nodo apagado..."
$tLectura = Measure-Command { $md5Caido = Md5Hdfs }
$disponible = ($md5Caido -eq $md5Original)
Write-Host "   md5 HDFS = $md5Caido · original = $md5Original · disponible = $disponible"

Write-Host "4) Esperando a que el NameNode declare muerto el DataNode..."
do { Start-Sleep -Seconds 5; $v = Vivos } while ($v -ge $vivosInicial -and ((Get-Date) - $t0).TotalSeconds -lt 900)
$tDetectado = ((Get-Date) - $t0).TotalSeconds
$fsckCaido = Nombres (Fsck)
Write-Host "   DataNodes vivos: $v (detectado a los $([math]::Round($tDetectado)) s)"

Write-Host "5) Esperando la re-replicación (todos los bloques con 2 réplicas vivas)..."
do { Start-Sleep -Seconds 5; $f = Fsck } while ($f -match "Under-replicated blocks:\s+[1-9]" -and ((Get-Date) - $t0).TotalSeconds -lt 900)
$tReplicado = ((Get-Date) - $t0).TotalSeconds
$fsckRecuperado = Nombres $f
Write-Host "   Re-replicado a los $([math]::Round($tReplicado)) s"

Write-Host "6) Encendiendo $Nodo de nuevo..."
docker start $Nodo | Out-Null
$t1 = Get-Date
do { Start-Sleep -Seconds 5; $v = Vivos } while ($v -lt $vivosInicial -and ((Get-Date) - $t1).TotalSeconds -lt 300)
$tReingreso = ((Get-Date) - $t1).TotalSeconds
Start-Sleep -Seconds 10
$fsckFinal = Nombres (Fsck)

$md = @"
# Tolerancia a fallos de HDFS (A3.4)

Generado por ``hadoop/hdfs/04_tolerancia.ps1`` el $(Get-Date -Format "yyyy-MM-dd HH:mm") (hora local).
Clúster: 1 NameNode + 3 DataNodes, ``dfs.replication = 2``. Archivo: ``$Ruta`` ($([math]::Round((Get-Item $Archivo).Length / 1MB, 2)) MB, bloques de $($Bloque/1MB) MB).
Nodo apagado: **$Nodo**.

| Paso | Resultado |
|---|---|
| DataNodes vivos al inicio | $vivosInicial |
| Lectura completa con el nodo apagado | **$(if ($disponible) { "OK" } else { "FALLÓ" })**: md5 $(if ($disponible) { "idéntico al" } else { "distinto del" }) original, $([math]::Round($tLectura.TotalSeconds, 1)) s ($script:avisosLectura avisos del cliente al intentar el nodo caído y pasar a otra réplica) |
| El NameNode declara muerto el DataNode | $([math]::Round($tDetectado)) s después de apagarlo (2 x recheck 30 s + 10 x latido 3 s = 90 s teóricos) |
| Todos los bloques vuelven a tener 2 réplicas vivas | $([math]::Round($tReplicado)) s después de apagarlo |
| El DataNode vuelve a reportarse al encenderlo | $([math]::Round($tReingreso)) s |

## Qué muestra

1. **Disponibilidad:** cada bloque tiene 2 réplicas en nodos distintos. Al caer un nodo, el cliente lee la otra réplica sin error: la lectura no se interrumpe.
2. **Detección:** el NameNode no se entera al instante. Marca el nodo como muerto cuando deja de recibir sus latidos durante el intervalo configurado.
3. **Auto-reparación:** los bloques que quedaron con 1 réplica (*under-replicated*) se copian a otro DataNode vivo hasta volver al factor 2, sin intervención manual.
4. **Reingreso:** al volver el nodo, sus réplicas sobran (*over-replicated*) y el NameNode elimina las excedentes.

## fsck antes de apagar el nodo
``````
$fsckInicial
``````

## fsck con el nodo apagado (tras ser declarado muerto)
``````
$fsckCaido
``````

## fsck después de la re-replicación
``````
$fsckRecuperado
``````

## fsck con el nodo de nuevo encendido
``````
$fsckFinal
``````
"@
New-Item -ItemType Directory -Force docs/resultados | Out-Null
$md | Out-File -Encoding utf8 docs/resultados/hdfs_tolerancia.md
Write-Host "Resultado en docs/resultados/hdfs_tolerancia.md"
