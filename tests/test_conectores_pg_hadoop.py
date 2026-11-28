"""A5.4 — Pruebas de conectores_postgres_hadoop.

* Unitarias: registro de consultas (cabeceras, parámetros), protección de sandbox.
* Integración (se omiten si PostgreSQL / HDFS no responden): las 17 consultas PG y las operaciones HDFS
  corren sin error; las funciones del dashboard respetan el contrato.
"""
from datetime import date
from functools import lru_cache

import pandas as pd
import pytest

import conectores_postgres_hadoop as a
from comun import Filtros
from contrato import COLUMNAS_ESTANDAR, ESTADOS_SENSOR, FUENTE_HDFS, FUENTE_POSTGRES, FUENTES_VALIDAS, ZONAS


# ------------------------------------------------------------------------------- unitarias
def test_registro_pg_completo():
    ids = set(a.CONSULTAS_PG)
    assert {f"PG-Q{i:02d}" for i in range(1, 11)} <= ids
    assert {f"PG-P{i:02d}" for i in range(1, 8)} <= ids
    for c in a.CONSULTAS_PG.values():
        assert c.tecnologia in FUENTES_VALIDAS and c.lenguaje == "sql" and c.titulo
        for nombre in c.parametros:                       # cada parámetro de la cabecera se usa en el SQL
            assert f"%({nombre})s" in c.codigo, (c.id, nombre)


def test_parametros_tipados():
    assert a.CONSULTAS_PG["PG-P07"].parametros["umbral"] == 15
    assert a.CONSULTAS_PG["PG-Q10"].parametros == {"desde": "2024-01-01", "hasta": "2024-12-31", "zona": ""}


def test_registro_hdfs():
    assert {"HD-OPS-LS", "HD-OPS-DU", "HD-OPS-FSCK"} <= set(a.CONSULTAS_HDFS)
    assert {f"HD-MR{i:02d}" for i in range(1, 6)} <= set(a.CONSULTAS_HDFS)
    assert "def main" in a.CONSULTAS_HDFS["HD-MR01"].codigo


@pytest.mark.parametrize("ruta", ["/smartcity/raw/openaq", "/smartcity", "/smartcity/sandbox/../raw", "/tmp"])
def test_sandbox_protegido(ruta):
    with pytest.raises(PermissionError):
        a.hdfs_rm(ruta)


# ------------------------------------------------------------------------------- integración
@lru_cache(maxsize=1)
def _pg():
    return a.estado().get(FUENTE_POSTGRES, False)


@lru_cache(maxsize=1)
def _hdfs():
    return a.estado().get(FUENTE_HDFS, False)


requiere_pg = pytest.mark.skipif("not _pg()", reason="PostgreSQL no disponible")
requiere_hdfs = pytest.mark.skipif("not _hdfs()", reason="HDFS no disponible")


@requiere_pg
@pytest.mark.parametrize("ident", sorted(a.CONSULTAS_PG))
def test_consultas_pg_corren(ident):
    df = a.ejecutar(ident)
    assert isinstance(df, pd.DataFrame)
    if ident.startswith("PG-Q") and ident not in ("PG-Q10",):     # catálogo: siempre hay filas
        assert not df.empty, ident


@requiere_pg
def test_explain_muestra_plan():
    assert "Execution Time" in a.explain("PG-Q01")


@requiere_pg
def test_sensores_y_kpis():
    df = a.sensores(Filtros(zonas=("Zona Norte",)))
    assert set(df["zona"]) == {"Zona Norte"} and set(df["estado"]) <= set(ESTADOS_SENSOR)
    assert df["lat"].dtype == float
    k = a.kpis_iot()
    assert k["sensores"] == k["activos"] + k["inactivos"] and k["fuente"] == FUENTE_POSTGRES


@requiere_pg
def test_lecturas_pg_contrato():
    f = Filtros(fecha_inicio=date(2024, 3, 1), fecha_fin=date(2024, 3, 31), tipos_metrica=("trafico", "energia"),
                perfil_horario="hora_pico")
    df = a.lecturas_pg(f, limite=500)
    assert list(df.columns) == COLUMNAS_ESTANDAR
    if not df.empty:
        assert set(df["zona"]) <= set(ZONAS) and set(df["fuente"]) == {FUENTE_POSTGRES}
        assert str(df["timestamp"].dt.tz) == "UTC"
        assert set(df["timestamp"].dt.hour) <= set(f.horas)


@requiere_hdfs
def test_hdfs_operaciones_sandbox():
    ruta = "/smartcity/sandbox/pytest/prueba.txt"
    a.hdfs_put(b"hola hdfs\n", ruta)
    df, comando = a.hdfs_ls("/smartcity/sandbox/pytest")
    assert comando.startswith("hdfs dfs -ls") and ruta in set(df["ruta"])
    assert a.hdfs_cat(ruta)[0].strip() == "hola hdfs"
    assert a.hdfs_get(ruta)[0] == b"hola hdfs\n"
    fsck, _ = a.hdfs_fsck(ruta)
    assert int(fsck["replicas_vivas"].iloc[0]) >= 1
    a.hdfs_rm("/smartcity/sandbox/pytest")


@requiere_hdfs
def test_resumen_hdfs():
    r = a.resumen_hdfs()
    assert r["datanodes_vivos"] >= 1 and r["fuente"] == FUENTE_HDFS


@requiere_pg
def test_lecturas_pg_metricas_d01():
    """D-01: sin tipo devuelve todas las variables con su código; «calidad_aire» es alias de PM2.5."""
    f = Filtros(fecha_inicio=date(2024, 3, 1), fecha_fin=date(2024, 3, 7))
    df = a.lecturas_pg(f, limite=2000)
    assert set(df["tipo_metrica"]) <= set(a.VARIABLES_PG)
    alias = a.lecturas_pg(Filtros(fecha_inicio=date(2024, 3, 1), fecha_fin=date(2024, 3, 7),
                                  tipos_metrica=("calidad_aire",)), limite=50)
    assert set(alias["tipo_metrica"]) <= {"calidad_aire"}
    solo_clima = a.sensores(Filtros(tipos_metrica=("temperatura",)))
    assert set(solo_clima["tipo_sensor"]) == {"clima"}
