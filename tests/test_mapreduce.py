"""A4.4 — Pruebas locales de los mappers/reducers (sin Hadoop): `cat muestra | mapper | sort | combiner | reducer`.

Los resultados se comparan con un cálculo independiente en Python sobre las mismas muestras (data/samples/).
"""
import json
import os
import subprocess
import sys
from collections import Counter, defaultdict
from pathlib import Path

import pytest

RAIZ = Path(__file__).resolve().parents[1]
MR = RAIZ / "hadoop" / "mapreduce"
LECTURAS = RAIZ / "data" / "samples" / "lectura_muestra.jsonl"
EVENTOS = RAIZ / "data" / "samples" / "evento_muestra.jsonl"


def correr(job, entrada: bytes, env=None, combinar=True):
    """Simula Hadoop Streaming: map -> sort -> (combiner) -> sort -> reduce."""
    entorno = {**os.environ, **(env or {})}
    py = sys.executable

    def paso(script, datos, *args):
        r = subprocess.run([py, str(MR / job / script), *args], input=datos, capture_output=True, env=entorno,
                           check=True)
        return r.stdout

    ordenar = lambda b: b"".join(sorted(b.splitlines(keepends=True)))  # noqa: E731
    salida = ordenar(paso("mapper.py", entrada))
    if combinar:
        salida = ordenar(paso("reducer.py", salida, "--combinar"))
    salida = paso("reducer.py", salida)
    return [ln.split("\t") for ln in salida.decode("utf-8").splitlines() if ln]


def documentos(ruta):
    docs = []
    for ln in ruta.read_text(encoding="utf-8").splitlines():
        try:
            docs.append(json.loads(ln))
        except ValueError:
            pass
    return docs


def test_mr01_mediciones_por_sensor():
    esperado = Counter(d["sensor_id"] for d in documentos(LECTURAS))
    resultado = {s: int(n) for s, n in correr("mr01", LECTURAS.read_bytes())}
    assert resultado == dict(esperado)


def test_mr02_top_sensores():
    mr01 = "".join(f"{s}\t{n}\n" for s, n in [("S-001", 5), ("S-002", 50), ("S-003", 7), ("S-004", 50)])
    filas = correr("mr02", mr01.encode(), env={"TOP_N": "3"}, combinar=False)
    assert [f[1] for f in filas] == ["S-004", "S-002", "S-003"]     # empate en 50: orden estable por id desc
    assert [f[0] for f in filas] == ["1", "2", "3"]


def test_mr03_temperatura_promedio_excluye_invalidas():
    temps = [d["mediciones"]["temperatura"] for d in documentos(LECTURAS)
             if "temperatura" in d.get("mediciones", {}) and (d.get("calidad_dato") or {}).get("valido", True)]
    filas = {(f[0], f[1], f[2]): (float(f[3]), int(f[4])) for f in correr("mr03", LECTURAS.read_bytes())}
    promedio, n = filas[("global", "-", "-")]
    assert n == len(temps) and promedio == pytest.approx(sum(temps) / len(temps), abs=1e-3)
    assert 99.0 not in temps                                             # la lectura marcada inválida no entra
    assert ("zona_anio", "Zona Sur", "2024") in filas


def test_mr04_eventos_por_zona():
    esperado = Counter(d["zona"]["nombre"] for d in documentos(EVENTOS))
    filas = correr("mr04", EVENTOS.read_bytes())
    por_zona = {f[1]: int(f[3]) for f in filas if f[0] == "zona"}
    assert por_zona == dict(esperado)
    assert sum(int(f[3]) for f in filas if f[0] == "zona_tipo") == sum(esperado.values())


def test_mr05_periodo_y_estadisticos():
    filas = correr("mr05", LECTURAS.read_bytes(), env={"DESDE": "2024-01-01", "HASTA": "2024-01-01"})
    assert {f[1] for f in filas} == {"2024-01-01"}                      # solo el día pedido
    esperado = defaultdict(list)
    for d in documentos(LECTURAS):
        ts = d["timestamp"]["$date"]
        if isinstance(ts, str) and ts.startswith("2024-01-01") and (d.get("calidad_dato") or {}).get("valido", True):
            for k, v in d["mediciones"].items():
                esperado[(d["sensor_id"], k)].append(v)
    for sensor, fecha, variable, prom, mn, mx, n in filas:
        valores = esperado[(sensor, variable)]
        assert int(n) == len(valores)
        assert float(prom) == pytest.approx(sum(valores) / len(valores), abs=1e-3)
        assert float(mn) == min(valores) and float(mx) == max(valores)
