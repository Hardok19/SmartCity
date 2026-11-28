"""C1/C3 — Pruebas del benchmark y de sus gráficos (sin servicios).

* El generador es determinista y la escala chica es prefijo de la grande (mismo dato lógico en todas las escalas).
* Los gráficos y los textos de hallazgos se construyen con un CSV mínimo y con el CSV real si existe.
"""
import json

import pandas as pd
import pytest

pytest.importorskip("psycopg2")
pytest.importorskip("pymongo")

import scripts.benchmark_comparacion as bench  # noqa: E402
from dashboard_app import graficos_experimentos as gx  # noqa: E402

CATALOGO = [("S-001", "Zona Norte", "calidad_aire"), ("S-006", "Zona Norte", "clima"),
            ("T-003", "Zona Centro", "trafico"), ("E-005", "Zona Oeste", "energia")]


@pytest.fixture
def directorio(tmp_path, monkeypatch):
    monkeypatch.setattr(bench, "DIR_DATOS", tmp_path)
    return tmp_path


def test_generador_determinista_y_prefijo(directorio):
    chico = pd.read_csv(bench.generar(400, CATALOGO))
    (directorio / "lectura_400.csv").rename(directorio / "copia.csv")
    otra_vez = pd.read_csv(bench.generar(400, CATALOGO))
    grande = pd.read_csv(bench.generar(1000, CATALOGO))
    pd.testing.assert_frame_equal(chico, otra_vez)
    pd.testing.assert_frame_equal(chico, grande.head(400))
    assert list(chico.columns) == ["id_sensor", "zona", "tipo_metrica", "timestamp", "valor"]
    assert (grande["valor"] >= 0).all()
    # 4 sensores: cada uno recibe una lectura cada 15 min
    assert grande.groupby("id_sensor").size().nunique() == 1
    paso = pd.to_datetime(grande.loc[grande["id_sensor"] == "S-001", "timestamp"]).diff().dropna().unique()
    assert len(paso) == 1 and pd.Timedelta(paso[0]) == pd.Timedelta(minutes=15)


def _csv_minimo(ruta):
    filas = []
    for exp, tec, var in [("E1", "PostgreSQL", "group_by_sql"), ("E1", "MongoDB", "aggregate_group"),
                          ("E1", "Spark", "dataframe_cores2"), ("E5", "Spark", "cores1"), ("E5", "Spark", "cores2"),
                          ("E7", "PostgreSQL", "con_indice"), ("E7", "PostgreSQL", "sin_indice")]:
        for i, n in enumerate((100_000, 1_000_000)):
            filas.append({"experimento": exp, "tecnologia": tec, "variante": var, "escala_filas": n,
                          "segundos": 0.01 * (i + 1) * (5 if tec == "Spark" else 1),
                          "detalle": json.dumps({}), "medido_en": "2026-11-27T00:00:00+00:00"})
    pd.DataFrame(filas).to_csv(ruta, index=False)


def test_figuras_y_hallazgos_con_csv_minimo(tmp_path):
    from dashboard_app.pagina_comparacion import hallazgo
    ruta = tmp_path / "benchmarks.csv"
    _csv_minimo(ruta)
    df = gx.cargar(ruta)
    assert len(gx.fig_e1(df).data) == 3 and len(gx.fig_e5(df).data) == 2
    assert "1 M" in hallazgo("E1", df) and "×" in hallazgo("E7", df)
    assert gx.fig_e6(df).data[0].x is None or len(gx.fig_e6(df).data[0].x) == 0   # sin E6: figura vacía


@pytest.mark.skipif(not gx.CSV.exists(), reason="sin docs/resultados/benchmarks.csv")
def test_csv_real_resultados_coinciden_entre_motores():
    df = gx.cargar()
    for exp, clave in (("E1", "suma"), ("E3", "total")):
        d = df[df["experimento"] == exp]
        for escala, g in d.groupby("escala_filas"):
            valores = {round(float(det[clave]), 2) for det in g["detalle"] if clave in det}
            assert len(valores) == 1, (exp, escala, valores)
    for exp, (_, funcion) in gx.FIGURAS.items():
        assert funcion(df).data, exp


def test_formatos():
    assert gx.escala_txt(5_000_000) == "5 M" and gx.escala_txt(100_000) == "100k"
    assert gx.tiempo_txt(0.0007) == "0,7 ms" and gx.tiempo_txt(4.2062) == "4,21 s"
