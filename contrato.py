"""
contrato.py — Contrato de datos compartido para el proyecto SmartCity Analytics (CE-3191)
NO modificar sin avisar al otro equipo. Cualquier cambio aquí afecta a ambos módulos
de conectores y al dashboard.

Cambios:
  2026-11-27 (D-01 / F1.1): TIPOS_METRICA incluye las variables concretas de Ambiente
    (pm25, pm10, co, no2, o3, co2, temperatura, humedad, precipitacion, viento), con su unidad en
    UNIDADES. Así una fila de COLUMNAS_ESTANDAR dice QUÉ se midió y el clima entra al contrato.
    Se conserva "calidad_aire" como categoría (equivale a PM2.5 cuando se pide como tipo, por
    compatibilidad). CATEGORIA_POR_METRICA indica el tipo de sensor que produce cada métrica
    (= tipos_sensor.tipo_sensor en PostgreSQL = campo `tipo` de la colección lectura en MongoDB).
"""

import pandas as pd

# =========================================================
# Catálogo de zonas (usar SIEMPRE estos strings exactos)
# =========================================================
ZONAS = [
    "Zona Norte",
    "Zona Sur",
    "Zona Centro",
    "Zona Este",
    "Zona Oeste",
]

# =========================================================
# Catálogo de tipos de métrica / sensor
# =========================================================
TIPOS_METRICA = [
    "calidad_aire",
    "trafico",
    "energia",
    "ruido",
    "ocupacion_parqueo",
    "transporte_publico",
    # D-01: variables concretas de Ambiente (mismos códigos que variables_medicion y que mediciones.* en Mongo)
    "pm25",
    "pm10",
    "co",
    "no2",
    "o3",
    "co2",
    "temperatura",
    "humedad",
    "precipitacion",
    "viento",
]

# Tipo de sensor que produce cada métrica (tipos_sensor en PostgreSQL, campo `tipo` en Mongo).
# "clima" es un tipo de sensor, no una métrica: sus métricas son temperatura, humedad, precipitacion y viento.
CATEGORIA_POR_METRICA = {
    "calidad_aire": "calidad_aire",
    "pm25": "calidad_aire", "pm10": "calidad_aire", "co": "calidad_aire", "no2": "calidad_aire",
    "o3": "calidad_aire", "co2": "calidad_aire",
    "temperatura": "clima", "humedad": "clima", "precipitacion": "clima", "viento": "clima",
    "trafico": "trafico", "energia": "energia", "ruido": "ruido",
    "ocupacion_parqueo": "ocupacion_parqueo", "transporte_publico": "transporte_publico",
}

# Métrica concreta que representa cada categoría cuando se pide la categoría como tipo (compatibilidad)
METRICA_DE_CATEGORIA = {"calidad_aire": "pm25"}

# =========================================================
# Estados válidos de sensor
# =========================================================
ESTADOS_SENSOR = ["activo", "inactivo"]

# =========================================================
# Nombres de fuente (para la columna 'fuente')
# =========================================================
FUENTE_POSTGRES = "PostgreSQL"
FUENTE_MONGO = "MongoDB"
FUENTE_HDFS = "HDFS"
FUENTE_SPARK = "Spark"

FUENTES_VALIDAS = [FUENTE_POSTGRES, FUENTE_MONGO, FUENTE_HDFS, FUENTE_SPARK]

# =========================================================
# Columnas estándar de salida (usar estos nombres, en este orden)
# =========================================================
COLUMNAS_ESTANDAR = ["zona", "timestamp", "id_sensor", "tipo_metrica", "valor", "fuente"]

# =========================================================
# Unidades por tipo de métrica
# =========================================================
UNIDADES = {
    "calidad_aire": "µg/m3",       # PM2.5 (ver METRICA_DE_CATEGORIA)
    "trafico": "vehiculos/hora",
    "energia": "kWh",
    "ruido": "dB",
    "ocupacion_parqueo": "%",
    "transporte_publico": "pasajeros",
    "pm25": "µg/m3",
    "pm10": "µg/m3",
    "co": "µg/m3",
    "no2": "µg/m3",
    "o3": "µg/m3",
    "co2": "ppm",
    "temperatura": "°C",
    "humedad": "%",
    "precipitacion": "mm",
    "viento": "km/h",
}


def tipos_sensor_de(metricas) -> list[str]:
    """Tipos de sensor que producen las métricas pedidas (para filtrar catálogos de sensores)."""
    return sorted({CATEGORIA_POR_METRICA[m] for m in metricas if m in CATEGORIA_POR_METRICA})

# =========================================================
# Formato y zona horaria de timestamp
# =========================================================
FORMATO_TIMESTAMP = "%Y-%m-%dT%H:%M:%S"  # ISO 8601
ZONA_HORARIA = "UTC"  # todo se guarda y compara en UTC


# =========================================================
# Funciones de validación — úsenlas en cada función de consulta
# antes de devolver el DataFrame
# =========================================================

def validar_zonas(df: pd.DataFrame, columna: str = "zona") -> None:
    """Lanza un error si aparece una zona fuera del catálogo."""
    zonas_invalidas = set(df[columna].unique()) - set(ZONAS)
    if zonas_invalidas:
        raise ValueError(
            f"Zonas fuera del catálogo acordado: {zonas_invalidas}. "
            f"Zonas válidas: {ZONAS}"
        )


def validar_tipo_metrica(df: pd.DataFrame, columna: str = "tipo_metrica") -> None:
    """Lanza un error si aparece un tipo de métrica fuera del catálogo."""
    tipos_invalidos = set(df[columna].unique()) - set(TIPOS_METRICA)
    if tipos_invalidos:
        raise ValueError(
            f"Tipos de métrica fuera del catálogo acordado: {tipos_invalidos}. "
            f"Tipos válidos: {TIPOS_METRICA}"
        )


def validar_estado(df: pd.DataFrame, columna: str = "estado") -> None:
    """Lanza un error si aparece un estado fuera del catálogo."""
    estados_invalidos = set(df[columna].unique()) - set(ESTADOS_SENSOR)
    if estados_invalidos:
        raise ValueError(
            f"Estados fuera del catálogo acordado: {estados_invalidos}. "
            f"Estados válidos: {ESTADOS_SENSOR}"
        )


def validar_columnas_estandar(df: pd.DataFrame) -> None:
    """Verifica que el DataFrame tenga exactamente las columnas estándar."""
    faltantes = set(COLUMNAS_ESTANDAR) - set(df.columns)
    if faltantes:
        raise ValueError(f"Faltan columnas obligatorias: {faltantes}")


def normalizar_timestamp(serie: pd.Series) -> pd.Series:
    """Convierte una serie a datetime en UTC, formato consistente."""
    return pd.to_datetime(serie, utc=True)


def ordenar_columnas_estandar(df: pd.DataFrame) -> pd.DataFrame:
    """Reordena el DataFrame para que respete el orden de COLUMNAS_ESTANDAR."""
    validar_columnas_estandar(df)
    return df[COLUMNAS_ESTANDAR]


# =========================================================
# Ejemplo de uso dentro de un módulo de conectores:
#
# from contrato import (
#     ZONAS, TIPOS_METRICA, FUENTE_MONGO,
#     validar_zonas, validar_tipo_metrica,
#     normalizar_timestamp, ordenar_columnas_estandar,
# )
#
# def consulta_temperatura_promedio_por_zona() -> pd.DataFrame:
#     df = pd.DataFrame(resultados_query)
#     df["timestamp"] = normalizar_timestamp(df["timestamp"])
#     df["fuente"] = FUENTE_MONGO
#     validar_zonas(df)
#     validar_tipo_metrica(df)
#     return ordenar_columnas_estandar(df)
# =========================================================