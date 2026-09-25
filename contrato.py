"""
contrato.py — Contrato de datos compartido para el proyecto SmartCity Analytics (CE-3191)
NO modificar sin avisar al otro equipo. Cualquier cambio aquí afecta a ambos módulos
de conectores y al dashboard.
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
]

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
    "calidad_aire": "µg/m3",       # PM2.5, PM10
    "trafico": "vehiculos/hora",
    "energia": "kWh",
    "ruido": "dB",
    "ocupacion_parqueo": "%",
    "transporte_publico": "pasajeros",
}

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