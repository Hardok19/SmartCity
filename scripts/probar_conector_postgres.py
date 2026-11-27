"""Prueba ejecutable del conector PostgreSQL sin Streamlit."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from contrato import COLUMNAS_ESTANDAR
from conectores_postgres import (estado, kpis_iot, sensores, lecturas_pg,
                                 kpis_ambiente, consultas_disponibles, ejecutar_consulta)

print("1. CONEXION", estado(), flush=True)
assert estado()["PostgreSQL"], "No se pudo conectar con PostgreSQL"

kpis = kpis_iot()
print("2. KPIs", kpis, flush=True)
assert kpis["sensores"] >= 40, kpis
assert kpis["mediciones"] >= 100000, kpis

catalogo = sensores()
print("3. CATALOGO", len(catalogo), "sensores, 5 zonas", flush=True)
assert len(catalogo) >= 40 and catalogo["zona"].nunique() == 5

muestra = lecturas_pg(limite=10)
print("4. LECTURAS CONTRATO\n", muestra.head(3).to_string(index=False), flush=True)
assert list(muestra.columns) == COLUMNAS_ESTANDAR
assert not muestra.empty
assert (muestra["fuente"] == "PostgreSQL").all()

print("5. PROMEDIOS AMBIENTALES\n", kpis_ambiente().to_string(index=False), flush=True)
reg = consultas_disponibles()
print("6. CONSULTAS REGISTRADAS", list(reg), flush=True)
for ident in sorted(reg):
    resultado = ejecutar_consulta(ident)
    print(f"   {ident}: {len(resultado)} filas", flush=True)
    assert len(resultado) > 0, f"Sin resultados: {ident}"
print("\nOK: 10 consultas y funciones del conector validadas", flush=True)
