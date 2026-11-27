<#
.SYNOPSIS
  B3.1 / D-04: ejecuta un job de spark/jobs dentro de smartcity_spark_master contra el clúster.

.EXAMPLE
  .\scripts\spark_submit.ps1 common
  .\scripts\spark_submit.ps1 analitica --analisis anomalias
  .\scripts\spark_submit.ps1 ejecutar_consultas --consultas SP-Q09 --exportar

.NOTES
  Los jobs que leen MongoDB necesitan MONGO_URI con host "mongo" (red Docker). Si no está definida en la
  sesión, se usa MONGO_URI_SPARK del .env o el valor por defecto del docker-compose.yml.
  La URI se pasa por variable de entorno (-e MONGO_URI), nunca en la línea de comandos.
#>
param(
    [Parameter(Mandatory = $true, Position = 0)][string]$Job,
    [Parameter(ValueFromRemainingArguments = $true)][string[]]$Argumentos
)

$ErrorActionPreference = "Stop"
$contenedor = if ($env:CONTENEDOR_SPARK) { $env:CONTENEDOR_SPARK } else { "smartcity_spark_master" }

if (-not $env:MONGO_URI -or $env:MONGO_URI -match "@localhost") {
    $uri = $null
    $envFile = Join-Path $PSScriptRoot "..\.env"
    if (Test-Path $envFile) {
        $linea = Select-String -Path $envFile -Pattern "^MONGO_URI_SPARK=(.+)$" | Select-Object -First 1
        if ($linea) { $uri = $linea.Matches[0].Groups[1].Value.Trim() }
    }
    if (-not $uri) { $uri = "mongodb://smartcity:smartcity123@mongo:27017/smartcity_db?authSource=admin" }
    $env:MONGO_URI = $uri
}

$script = "/opt/smartcity/spark/jobs/$Job.py"
$cmd = @(
    "exec", "--user", "0:0", "-e", "HOME=/tmp", "-e", "HADOOP_USER_NAME=root", "-e", "MONGO_URI",
    $contenedor, "/opt/bitnami/spark/bin/spark-submit",
    "--master", "spark://spark-master:7077", "--total-executor-cores", "2",
    "--conf", "spark.jars.ivy=/tmp/.ivy2",
    "--packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.4.1,org.postgresql:postgresql:42.7.4",
    "--py-files", "/opt/smartcity/spark/jobs/common.py,/opt/smartcity/spark/jobs/rutas.py",
    $script
) + $Argumentos

Write-Host "docker $($cmd -join ' ')" -ForegroundColor DarkGray
# Spark escribe sus logs en stderr: en PowerShell 5.1 eso no debe tratarse como error.
$ErrorActionPreference = "Continue"
& docker @cmd 2>&1 | ForEach-Object { "$_" }
exit $LASTEXITCODE
