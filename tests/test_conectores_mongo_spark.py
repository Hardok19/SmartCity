"""B5.4 — Pruebas de conectores_mongo_spark.

* Unitarias (sin servicios): registro de consultas, Filtros, contrato en tráfico/energía.
* Integración (se omiten si MongoDB / HDFS no responden): las 25 consultas corren sin error y las
  funciones del dashboard respetan el contrato (COLUMNAS_ESTANDAR, zonas válidas, timestamp UTC).

    pytest -q tests/test_conectores_mongo_spark.py
"""
import subprocess
import time
from datetime import date
from functools import lru_cache

import pandas as pd
import pytest

import conectores_mongo_spark as m
from comun import Filtros
from contrato import COLUMNAS_ESTANDAR, FUENTE_MONGO, FUENTE_SPARK, FUENTES_VALIDAS, ZONAS

F_DEFECTO = Filtros(fecha_inicio=date(2024, 4, 1), fecha_fin=date(2024, 4, 30))


# ------------------------------------------------------------------------------- unitarias
def test_registro_consultas_completo():
    assert [f"MG-Q{i:02d}" for i in range(1, 11)] == [k for k in m.CONSULTAS_MONGO if k.startswith("MG-Q")]
    assert [f"MG-P{i:02d}" for i in range(1, 6)] == [k for k in m.CONSULTAS_MONGO if k.startswith("MG-P")]
    assert [f"SP-Q{i:02d}" for i in range(1, 11)] == list(m.CONSULTAS_SPARK)
    for c in [*m.CONSULTAS_MONGO.values(), *m.CONSULTAS_SPARK.values()]:
        assert c.tecnologia in FUENTES_VALIDAS
        assert c.codigo and "no encontrado" not in c.codigo, c.id
        assert callable(c.funcion)


def test_filtros_validan():
    with pytest.raises(ValueError):
        Filtros(perfil_horario="siesta")
    with pytest.raises(ValueError):
        Filtros(fuente="Oracle")
    with pytest.raises(ValueError):
        Filtros(fecha_inicio=date(2024, 2, 1), fecha_fin=date(2024, 1, 1))
    assert Filtros(perfil_horario="hora_pico").horas == (6, 7, 8, 16, 17, 18, 19)


def test_rango_fechas_inclusivo():
    r = m._rango("timestamp", "2024-04-01", "2024-04-01")["timestamp"]
    assert r["$gte"].isoformat() == "2024-04-01T00:00:00+00:00"
    assert r["$lte"].isoformat().startswith("2024-04-01T23:59:59.999")


def _parquet_falso():
    ts = pd.date_range("2024-04-01", periods=48, freq="h", tz="UTC")
    filas = []
    for z in ZONAS:
        for t in ts:
            filas.append({"zona": z, "timestamp": t, "id_sensor": "T-001", "tipo_metrica": "trafico", "valor": 500.0,
                          "fuente": "Spark", "origen": "sintetico", "velocidad_promedio_kmh": 40.0,
                          "congestion_pct": 20.0, "anio": 2024, "mes": 4})
            filas.append({"zona": z, "timestamp": t, "id_sensor": "E-001", "tipo_metrica": "energia",
                          "valor": 9e4 + t.hour, "fuente": "Spark", "origen": "sintetico",
                          "velocidad_promedio_kmh": None, "congestion_pct": None, "anio": 2024, "mes": 4})
    return pd.DataFrame(filas)


def test_trafico_energia_respeta_contrato(monkeypatch):
    monkeypatch.setattr(m, "leer_parquet_hdfs", lambda ruta: _parquet_falso())
    m._CACHE_TE.clear()
    f = Filtros(fecha_inicio=date(2024, 4, 1), fecha_fin=date(2024, 4, 1), perfil_horario="hora_pico",
                zonas=("Zona Norte",), tipos_metrica=("trafico",))
    df = m.lecturas_trafico_energia(f)
    assert list(df.columns) == COLUMNAS_ESTANDAR
    assert set(df["zona"]) == {"Zona Norte"} and set(df["tipo_metrica"]) == {"trafico"}
    assert set(df["timestamp"].dt.hour) <= set(f.horas)
    assert str(df["timestamp"].dt.tz) == "UTC" and set(df["fuente"]) == {FUENTE_SPARK}
    k = m.kpis_energia(Filtros())
    assert k["hora_pico_utc"] == 23 and k["fuente"] == FUENTE_SPARK
    assert k["consumo_promedio_por_zona_kwh"] == pytest.approx(k["consumo_total_kwh"] / 5)
    assert k["variacion_pct_periodo_anterior"] is None          # sin rango de fechas no hay período anterior
    k2 = m.kpis_energia(Filtros(fecha_inicio=date(2024, 4, 2), fecha_fin=date(2024, 4, 2)))
    assert k2["variacion_pct_periodo_anterior"] == pytest.approx(0.0)   # mismo perfil el día anterior
    assert m.kpis_movilidad(Filtros())["vehiculos_detectados"] == 500.0 * 48 * 5
    m._CACHE_TE.clear()


# ------------------------------------------------------------------------------- integración
@lru_cache(maxsize=1)
def _mongo_arriba():
    return m.estado()[FUENTE_MONGO]


@lru_cache(maxsize=1)
def _mongo_con_datos():
    return _mongo_arriba() and m.db().lectura.estimated_document_count() > 0


@lru_cache(maxsize=1)
def _hdfs_arriba():
    try:
        return subprocess.run(["docker", "exec", m.CONFIG.contenedor_namenode, "hdfs", "dfs", "-test", "-d",
                               m.RUTAS_HDFS["analitica"]], capture_output=True, timeout=60).returncode == 0
    except Exception:  # noqa: BLE001
        return False


requiere_mongo = pytest.mark.skipif("not _mongo_con_datos()", reason="MongoDB no disponible o sin datos cargados")
requiere_mongo_vacio_ok = pytest.mark.skipif("not _mongo_arriba()", reason="MongoDB no disponible")
requiere_hdfs = pytest.mark.skipif("not _hdfs_arriba()", reason="HDFS/namenode no disponible")


@requiere_mongo
@pytest.mark.parametrize("ident", list(m.CONSULTAS_MONGO))
def test_consultas_mongo_por_defecto(ident):
    t0 = time.perf_counter()
    df = m.ejecutar(ident)
    segundos = time.perf_counter() - t0
    assert isinstance(df, pd.DataFrame) and not df.empty, f"{ident} sin filas"
    assert segundos < 3, f"{ident} tardó {segundos:.1f} s (aceptación B2.4: < 3 s)"


@requiere_mongo
def test_lecturas_mongo_contrato():
    df = m.lecturas_mongo(F_DEFECTO, limite=500)
    assert list(df.columns) == COLUMNAS_ESTANDAR
    assert set(df["zona"]) <= set(ZONAS) and set(df["fuente"]) == {FUENTE_MONGO}
    assert str(df["timestamp"].dt.tz) == "UTC"


@requiere_mongo
def test_funciones_dashboard_mongo():
    assert m.kpis_ambiente(F_DEFECTO)["fuente"] == FUENTE_MONGO
    assert m.kpis_iot_mongo(F_DEFECTO)["mediciones_recibidas"] > 0
    assert m.kpis_incidentes(F_DEFECTO)["incidentes"] > 0
    for fn in (m.serie_temporal, m.comparativa_zonas, m.distribucion_tipos, m.sensores_mongo,
               m.alertas, m.eventos, m.incidentes):
        df = fn(F_DEFECTO)
        assert not df.empty and set(df["fuente"]) == {FUENTE_MONGO}, fn.__name__
        if "zona" in df:
            assert set(df["zona"].dropna()) <= set(ZONAS), fn.__name__


@requiere_mongo_vacio_ok
def test_crud_demo():
    for op in m.OPERACIONES_CRUD:
        resultado, comando = m.crud(op)
        assert comando.startswith("db.demo_crud."), op


@requiere_hdfs
@pytest.mark.parametrize("ident", list(m.CONSULTAS_SPARK))
def test_consultas_spark_resultado_guardado(ident):
    df = m.ejecutar(ident)  # lee el último resultado exportado con ejecutar_consultas.py --exportar
    assert isinstance(df, pd.DataFrame) and not df.empty


@requiere_hdfs
def test_spark_top_zonas_pico():
    df = m.spark_top_zonas_pico()
    assert len(df) <= 3 and set(df["zona"]) <= set(ZONAS)
