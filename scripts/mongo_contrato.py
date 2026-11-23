"""mongo_contrato.py — Puente MongoDB -> contrato.py, SIN modificar contrato.py.

Problema: el contrato solo conoce TIPOS_METRICA = calidad_aire, trafico, energia, ruido, ocupacion_parqueo,
transporte_publico (no hay `clima`) y UN valor por fila. Mongo guarda varias variables por documento
(`mediciones: {pm25, pm10, ...}`) y también lecturas de `clima`.

Regla acordada (para ambos módulos de conectores):
  * Todo DataFrame que cumpla COLUMNAS_ESTANDAR (lecturas_mongo, lecturas_pg, lecturas_hdfs, ...) contiene
    SOLO tipos del contrato. Para calidad_aire, `valor` = PM2.5 (µg/m3, ver UNIDADES).
  * El clima (temperatura, humedad, precipitación, viento) NO sale por COLUMNAS_ESTANDAR: se sirve por
    funciones de KPI/gráfico propias (kpis_ambiente, etc.), con su fuente indicada igualmente.

Uso desde el conector:
    from scripts.mongo_contrato import pipeline_lecturas, a_contrato
    docs = db["lectura"].aggregate(pipeline_lecturas(fecha_inicio=..., fecha_fin=..., zonas=[...]))
    df = a_contrato(docs)           # validado contra contrato.py
"""
import pandas as pd

from contrato import (
    FUENTE_MONGO, TIPOS_METRICA, normalizar_timestamp, ordenar_columnas_estandar,
    validar_tipo_metrica, validar_zonas,
)

# Tipo del contrato -> variable de `mediciones` que se reporta como `valor`
VARIABLE_VALOR = {"calidad_aire": "pm25"}
# Tipos que Mongo guarda pero el contrato no conoce
TIPOS_FUERA_DE_CONTRATO = {"clima"}


def pipeline_lecturas(fecha_inicio=None, fecha_fin=None, zonas=None, id_sensor=None,
                      tipos=("calidad_aire",), limite=None):
    """Pipeline de agregación sobre `lectura` que devuelve ya las columnas estándar (sin `fuente`)."""
    tipos = list(tipos)
    for t in tipos:
        if t in TIPOS_FUERA_DE_CONTRATO:
            raise ValueError(f"'{t}' no está en el contrato (TIPOS_METRICA): se sirve por kpis_ambiente(), no por lecturas_*().")
        if t not in TIPOS_METRICA or t not in VARIABLE_VALOR:
            raise ValueError(f"Tipo '{t}' sin variable de valor definida en VARIABLE_VALOR (soportados: {list(VARIABLE_VALOR)}).")
    if len(tipos) != 1:
        raise ValueError("Pide un tipo por llamada (cada tipo usa una variable distinta).")
    tipo = tipos[0]
    variable = VARIABLE_VALOR[tipo]

    match = {"tipo": tipo, f"mediciones.{variable}": {"$type": "number"}}
    if fecha_inicio or fecha_fin:
        match["timestamp"] = {**({"$gte": fecha_inicio} if fecha_inicio else {}),
                              **({"$lt": fecha_fin} if fecha_fin else {})}
    if zonas:
        match["ubicacion.zona"] = {"$in": list(zonas)}
    if id_sensor:
        match["sensor_id"] = id_sensor

    pipeline = [
        {"$match": match},
        {"$project": {"_id": 0, "zona": "$ubicacion.zona", "timestamp": 1, "id_sensor": "$sensor_id",
                      "tipo_metrica": "$tipo", "valor": f"$mediciones.{variable}"}},
    ]
    if limite:
        pipeline.append({"$limit": int(limite)})
    return pipeline


def a_contrato(docs) -> pd.DataFrame:
    """Convierte la salida de pipeline_lecturas() en un DataFrame que cumple el contrato."""
    df = pd.DataFrame(list(docs), columns=["zona", "timestamp", "id_sensor", "tipo_metrica", "valor"])
    df["timestamp"] = normalizar_timestamp(df["timestamp"])
    df["fuente"] = FUENTE_MONGO
    validar_zonas(df)
    validar_tipo_metrica(df)
    return ordenar_columnas_estandar(df)
