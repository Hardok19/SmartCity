"""Rutas HDFS finales de Spark (spark/jobs/rutas.py) y validación de las salidas de los jobs (sin servicios)."""
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "spark" / "jobs"))
pytest.importorskip("pyspark")

import common  # noqa: E402
from rutas import RUTAS  # noqa: E402


def test_capas_finales():
    assert RUTAS["ambiente"] == "/smartcity/curated/ambiente"
    assert RUTAS["ambiente_hdfs"].startswith("/smartcity/curated/")
    assert RUTAS["trafico_energia"].startswith("/smartcity/curated/")
    assert RUTAS["analitica"] == "/smartcity/analytics"
    assert RUTAS["consultas"].startswith("/smartcity/analytics/")
    assert RUTAS["reporte_q10"].startswith("/smartcity/analytics/")
    assert RUTAS["optimizacion"].startswith("/smartcity/sandbox/")          # experimento B4.3
    assert not any("/sandbox/" in v for k, v in RUTAS.items() if k != "optimizacion")


def test_rutas_con_esquema_hdfs():
    assert all(v.startswith("hdfs://namenode:8020/smartcity") for v in common.RUTAS.values())
    assert common.CATALOGO_SENSORES.startswith("file:///")                 # get_spark() fija defaultFS = HDFS


@pytest.mark.parametrize("ruta,ok", [
    ("hdfs://namenode:8020/smartcity/curated/ambiente", True),
    ("hdfs://namenode:8020/smartcity/analytics", True),
    ("hdfs://namenode:8020/smartcity/analytics/descriptivo/", True),
    ("hdfs://namenode:8020/smartcity/sandbox/prueba", True),
    ("hdfs://namenode:8020/smartcity/raw/openaq", False),
    ("hdfs://namenode:8020/smartcity/mapreduce/mr01", False),
    ("hdfs://namenode:8020/smartcity", False),
    ("hdfs://namenode:8020/smartcity/analytics2", False),
])
def test_salida_permitida(ruta, ok):
    assert common.salida_permitida(ruta) is ok
