"""F1.1 / D-01 — Contrato de datos ampliado con las métricas de Ambiente (sin servicios)."""
import pandas as pd
import pytest

import contrato as c
from scripts.mongo_contrato import METRICAS_MONGO, a_contrato, pipeline_lecturas, variable_de


def test_cada_metrica_tiene_unidad_y_categoria():
    assert set(c.UNIDADES) == set(c.TIPOS_METRICA)
    assert set(c.CATEGORIA_POR_METRICA) == set(c.TIPOS_METRICA)
    assert len(c.TIPOS_METRICA) == len(set(c.TIPOS_METRICA))
    assert c.UNIDADES["temperatura"] == "°C" and c.UNIDADES["co2"] == "ppm" and c.UNIDADES["pm25"] == "µg/m3"


def test_tipos_sensor_de():
    assert c.tipos_sensor_de(["pm25", "no2"]) == ["calidad_aire"]
    assert c.tipos_sensor_de(["temperatura", "trafico"]) == ["clima", "trafico"]
    assert c.tipos_sensor_de([]) == [] and c.tipos_sensor_de(["desconocida"]) == []


def test_validar_tipo_metrica_acepta_clima_y_rechaza_desconocidos():
    c.validar_tipo_metrica(pd.DataFrame({"tipo_metrica": ["pm25", "temperatura", "trafico", "calidad_aire"]}))
    with pytest.raises(ValueError):
        c.validar_tipo_metrica(pd.DataFrame({"tipo_metrica": ["clima"]}))      # clima es tipo de sensor, no métrica


@pytest.mark.parametrize("metrica,tipo_doc,variable", [("pm25", "calidad_aire", "pm25"),
                                                       ("temperatura", "clima", "temperatura"),
                                                       ("calidad_aire", "calidad_aire", "pm25")])
def test_pipeline_mongo_por_metrica(metrica, tipo_doc, variable):
    match, proyeccion = pipeline_lecturas(zonas=["Zona Sur"], tipos=(metrica,))
    assert match["$match"]["tipo"] == tipo_doc
    assert f"mediciones.{variable}" in match["$match"]
    assert proyeccion["$project"]["valor"] == f"$mediciones.{variable}"
    assert proyeccion["$project"]["tipo_metrica"] == {"$literal": metrica}


def test_pipeline_mongo_rechaza_metricas_ajenas():
    for mala in ("trafico", "clima", "x"):
        with pytest.raises(ValueError):
            pipeline_lecturas(tipos=(mala,))
    with pytest.raises(ValueError):
        pipeline_lecturas(tipos=("pm25", "pm10"))
    assert set(METRICAS_MONGO) <= set(c.TIPOS_METRICA) and variable_de("calidad_aire") == "pm25"


def test_a_contrato_con_metricas_nuevas():
    docs = [{"zona": "Zona Norte", "timestamp": "2024-01-01T00:00:00Z", "id_sensor": "S-006",
             "tipo_metrica": "temperatura", "valor": 5.0}]
    df = a_contrato(docs)
    assert list(df.columns) == c.COLUMNAS_ESTANDAR and df["fuente"].iloc[0] == c.FUENTE_MONGO
    assert str(df["timestamp"].dt.tz) == "UTC"
