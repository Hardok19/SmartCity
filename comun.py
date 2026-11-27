"""comun.py — Piezas compartidas por los dos módulos de conectores y el dashboard (F1.3).

Archivo compartido por ambas parejas: cambios solo con aviso a la otra pareja.

Contiene:
  * ``Filtros``   — filtros globales del dashboard (todas las funciones de conectores los reciben).
  * ``Consulta``  — descripción de una consulta ejecutable desde la UI (PG-Q01, MG-Q05, HD-MR02, SP-Q07…).
  * ``CONFIG``    — credenciales y direcciones leídas de ``.env`` (ver ``.env.example``).
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import date, datetime, time, timezone
from typing import Any, Callable, Optional

import pandas as pd

from contrato import FUENTES_VALIDAS

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:  # python-dotenv es opcional: también sirven variables de entorno normales
    pass


# =========================================================
# Perfiles horarios (horas UTC, inclusive)
# =========================================================
PERFILES_HORARIOS: dict[str, tuple[int, ...]] = {
    "todo_el_dia": tuple(range(0, 24)),
    "madrugada": tuple(range(0, 6)),
    "manana": tuple(range(6, 12)),
    "tarde": tuple(range(12, 18)),
    "noche": tuple(range(18, 24)),
    "hora_pico": tuple(range(6, 9)) + tuple(range(16, 20)),
}

ETIQUETAS_PERFIL = {
    "todo_el_dia": "Todo el día",
    "madrugada": "Madrugada (0–5 h)",
    "manana": "Mañana (6–11 h)",
    "tarde": "Tarde (12–17 h)",
    "noche": "Noche (18–23 h)",
    "hora_pico": "Hora pico (6–8 h y 16–19 h)",
}


def _a_datetime_utc(valor: Any, fin_de_dia: bool = False) -> Optional[datetime]:
    if valor is None:
        return None
    if isinstance(valor, datetime):
        return valor if valor.tzinfo else valor.replace(tzinfo=timezone.utc)
    if isinstance(valor, date):
        return datetime.combine(valor, time.max if fin_de_dia else time.min, tzinfo=timezone.utc)
    return pd.Timestamp(valor, tz="UTC").to_pydatetime()


@dataclass(frozen=True)
class Filtros:
    """Filtros globales. Inmutable y con tuplas para poder usarse como clave de caché de Streamlit."""

    fecha_inicio: Optional[date] = None
    fecha_fin: Optional[date] = None
    perfil_horario: str = "todo_el_dia"
    zonas: tuple[str, ...] = ()
    tipos_metrica: tuple[str, ...] = ()
    id_sensor: Optional[str] = None
    tipo_evento: Optional[str] = None
    fuente: Optional[str] = None
    valor_min: Optional[float] = None
    valor_max: Optional[float] = None

    def __post_init__(self):
        if self.perfil_horario not in PERFILES_HORARIOS:
            raise ValueError(f"perfil_horario inválido: {self.perfil_horario}. Válidos: {list(PERFILES_HORARIOS)}")
        if self.fuente is not None and self.fuente not in FUENTES_VALIDAS:
            raise ValueError(f"fuente inválida: {self.fuente}. Válidas: {FUENTES_VALIDAS}")
        if self.fecha_inicio and self.fecha_fin and self.fecha_inicio > self.fecha_fin:
            raise ValueError("fecha_inicio es posterior a fecha_fin")

    # --- ayudas para los conectores -------------------------------------------------------
    @property
    def horas(self) -> tuple[int, ...]:
        """Horas UTC incluidas por el perfil horario."""
        return PERFILES_HORARIOS[self.perfil_horario]

    @property
    def desde_utc(self) -> Optional[datetime]:
        """Inicio del rango (00:00:00 UTC de ``fecha_inicio``)."""
        return _a_datetime_utc(self.fecha_inicio)

    @property
    def hasta_utc(self) -> Optional[datetime]:
        """Fin del rango, inclusive (23:59:59.999999 UTC de ``fecha_fin``)."""
        return _a_datetime_utc(self.fecha_fin, fin_de_dia=True)

    def aplicar(self, df: pd.DataFrame, col_timestamp: str = "timestamp", col_zona: str = "zona",
                col_valor: Optional[str] = "valor", col_sensor: Optional[str] = "id_sensor",
                col_tipo: Optional[str] = "tipo_metrica") -> pd.DataFrame:
        """Aplica los filtros a un DataFrame ya cargado (para fuentes que no filtran en origen).

        Las columnas que no existan en ``df`` se ignoran.
        """
        if df.empty:
            return df
        m = pd.Series(True, index=df.index)
        if col_timestamp in df.columns:
            ts = pd.to_datetime(df[col_timestamp], utc=True)
            if self.desde_utc is not None:
                m &= ts >= pd.Timestamp(self.desde_utc)
            if self.hasta_utc is not None:
                m &= ts <= pd.Timestamp(self.hasta_utc)
            if self.perfil_horario != "todo_el_dia":
                m &= ts.dt.hour.isin(self.horas)
        if self.zonas and col_zona in df.columns:
            m &= df[col_zona].isin(self.zonas)
        if self.tipos_metrica and col_tipo and col_tipo in df.columns:
            m &= df[col_tipo].isin(self.tipos_metrica)
        if self.id_sensor and col_sensor and col_sensor in df.columns:
            m &= df[col_sensor] == self.id_sensor
        if col_valor and col_valor in df.columns:
            if self.valor_min is not None:
                m &= df[col_valor] >= self.valor_min
            if self.valor_max is not None:
                m &= df[col_valor] <= self.valor_max
        return df[m]


@dataclass
class Consulta:
    """Consulta mostrable y ejecutable desde la sección "Consultas por tecnología" del dashboard."""

    id: str                                   # PG-Q01, MG-Q05, HD-MR02, SP-Q07…
    tecnologia: str                           # una de contrato.FUENTES_VALIDAS
    titulo: str
    descripcion: str
    codigo: str                               # SQL / pipeline / mapper / SQL Spark (se muestra en la UI)
    lenguaje: str                             # para st.code: "sql", "javascript", "python", "bash"…
    parametros: dict[str, Any] = field(default_factory=dict)   # nombre -> valor por defecto
    funcion: Optional[Callable[..., pd.DataFrame]] = None

    def __post_init__(self):
        if self.tecnologia not in FUENTES_VALIDAS:
            raise ValueError(f"tecnologia inválida en {self.id}: {self.tecnologia}")

    def ejecutar(self, **params) -> pd.DataFrame:
        if self.funcion is None:
            raise NotImplementedError(f"{self.id} no tiene función asociada")
        argumentos = {**self.parametros, **params}
        return self.funcion(**argumentos)


# =========================================================
# Configuración (.env)
# =========================================================
def _env(nombre: str, defecto: str) -> str:
    return os.environ.get(nombre, defecto)


@dataclass(frozen=True)
class Config:
    # PostgreSQL
    pg_host: str = _env("PG_HOST", "localhost")
    pg_port: int = int(_env("PG_PORT", "5433"))
    pg_db: str = _env("PG_DB", "smartcity_db")
    pg_user: str = _env("PG_USER", "smartcity")
    pg_password: str = _env("PG_PASSWORD", "smartcity123")
    # MongoDB (desde el anfitrión; dentro de Docker el host es "mongo")
    mongo_uri: str = _env("MONGO_URI", "mongodb://smartcity:smartcity123@localhost:27017/smartcity_db?authSource=admin")
    mongo_db: str = _env("MONGO_DB", "smartcity_db")
    # HDFS
    hdfs_web: str = _env("HDFS_WEB", "http://localhost:9870")
    hdfs_user: str = _env("HDFS_USER", "root")
    # Contenedores Docker
    contenedor_namenode: str = _env("CONTENEDOR_NAMENODE", "smartcity_namenode")
    contenedor_spark: str = _env("CONTENEDOR_SPARK", "smartcity_spark_master")
    spark_ui: str = _env("SPARK_UI", "http://localhost:8080")
    # MONGO_URI que usan los jobs Spark DENTRO de Docker (host "mongo")
    mongo_uri_spark: str = _env("MONGO_URI_SPARK", "mongodb://smartcity:smartcity123@mongo:27017/smartcity_db?authSource=admin")

    @property
    def pg_url(self) -> str:
        return f"postgresql+psycopg2://{self.pg_user}:{self.pg_password}@{self.pg_host}:{self.pg_port}/{self.pg_db}"


CONFIG = Config()
