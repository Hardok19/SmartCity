"""mongo_contrato.py — Puente MongoDB -> contrato.py (COLUMNAS_ESTANDAR).

Mongo guarda varias variables por documento (`mediciones: {pm25, pm10, ...}`) y el `tipo` del documento es el tipo
de sensor (calidad_aire, clima, ...). El contrato (D-01, 2026-11-27) tiene una fila por métrica: `tipo_metrica` es la
variable medida (pm25, temperatura, ...), con su unidad en contrato.UNIDADES.

Regla para ambos módulos de conectores:
  * Se pide UNA métrica por pipeline: `valor` = `mediciones.<métrica>` de los documentos cuyo `tipo` es
    contrato.CATEGORIA_POR_METRICA[métrica] (pm25 -> calidad_aire, temperatura -> clima, ...).
  * La categoría "calidad_aire" se sigue aceptando por compatibilidad y equivale a PM2.5
    (contrato.METRICA_DE_CATEGORIA); en ese caso la fila sale con tipo_metrica = "calidad_aire".

Uso desde el conector:
    from scripts.mongo_contrato import pipeline_lecturas, a_contrato
    docs = db["lectura"].aggregate(pipeline_lecturas(fecha_inicio=..., fecha_fin=..., zonas=[...], tipos=("pm25",)))
    df = a_contrato(docs)           # validado contra contrato.py
"""
import pandas as pd

from contrato import (
    CATEGORIA_POR_METRICA, FUENTE_MONGO, METRICA_DE_CATEGORIA, TIPOS_METRICA, normalizar_timestamp,
    ordenar_columnas_estandar, validar_tipo_metrica, validar_zonas,
)

# Métricas que existen como campo de `mediciones` en la colección lectura
METRICAS_MONGO = ["pm25", "pm10", "co", "no2", "o3", "co2", "temperatura", "humedad", "precipitacion", "viento"]


def variable_de(metrica: str) -> str:
    """Campo de `mediciones` que da el valor de la métrica pedida (calidad_aire -> pm25)."""
    variable = METRICA_DE_CATEGORIA.get(metrica, metrica)
    if metrica not in TIPOS_METRICA or variable not in METRICAS_MONGO:
        raise ValueError(f"'{metrica}' no es una métrica de las lecturas de Mongo (válidas: {METRICAS_MONGO + ['calidad_aire']}).")
    return variable


def pipeline_lecturas(fecha_inicio=None, fecha_fin=None, zonas=None, id_sensor=None,
                      tipos=("pm25",), limite=None):
    """Pipeline de agregación sobre `lectura` que devuelve ya las columnas estándar (sin `fuente`).

    Devuelve [$match, $project] (+ $limit): el conector puede intercalar etapas entre ambas.
    """
    tipos = list(tipos)
    if len(tipos) != 1:
        raise ValueError("Pide una métrica por llamada (cada métrica usa un campo distinto de `mediciones`).")
    metrica = tipos[0]
    variable = variable_de(metrica)

    match = {"tipo": CATEGORIA_POR_METRICA[variable], f"mediciones.{variable}": {"$type": "number"}}
    if fecha_inicio or fecha_fin:
        match["timestamp"] = {**({"$gte": fecha_inicio} if fecha_inicio else {}),
                              **({"$lte": fecha_fin} if fecha_fin else {})}
    if zonas:
        match["ubicacion.zona"] = {"$in": list(zonas)}
    if id_sensor:
        match["sensor_id"] = id_sensor

    pipeline = [
        {"$match": match},
        {"$project": {"_id": 0, "zona": "$ubicacion.zona", "timestamp": 1, "id_sensor": "$sensor_id",
                      "tipo_metrica": {"$literal": metrica}, "valor": f"$mediciones.{variable}"}},
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
