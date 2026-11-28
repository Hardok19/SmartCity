#!/usr/bin/env python3
"""C3 — Exporta a PNG los gráficos de los experimentos (los mismos que muestra el dashboard).

Lee docs/resultados/benchmarks.csv, arma cada figura con dashboard_app/graficos_experimentos.py y la captura con
Edge o Chrome sin ventana (scripts/navegador.py). Salida: docs/resultados/img/e1.png … e7.png.

  python scripts/graficos_benchmarks.py
"""
import sys
from pathlib import Path

import plotly.io as pio

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ))
sys.path.insert(0, str(RAIZ / "scripts"))
from dashboard_app.graficos_experimentos import FIGURAS, cargar  # noqa: E402
from navegador import HERRAMIENTAS, html_a_png  # noqa: E402

SALIDA = RAIZ / "docs" / "resultados" / "img"


def main():
    df = cargar()
    if df.empty:
        print("No hay docs/resultados/benchmarks.csv: ejecute antes scripts/benchmark_comparacion.py")
        return
    HERRAMIENTAS.mkdir(parents=True, exist_ok=True)
    for exp, (titulo, funcion) in FIGURAS.items():
        fig = funcion(df)
        if not fig.data:
            print(f"  {exp}: sin datos")
            continue
        fig.update_layout(title=dict(text=f"{exp} · {titulo}", x=0, font=dict(size=15)), width=1000,
                          margin=dict(t=90))
        fig.update_layout(legend=dict(y=1.12))
        pagina = HERRAMIENTAS / f"grafico_{exp.lower()}.html"
        pagina.write_text(pio.to_html(fig, include_plotlyjs=True, full_html=True,
                                      config={"staticPlot": True}), encoding="utf-8")
        png = html_a_png(pagina, SALIDA / f"{exp.lower()}.png", ancho=1040, alto=int(fig.layout.height or 400) + 60,
                         escala=2, espera_ms=4000)
        print(f"-> {png.relative_to(RAIZ).as_posix()}")


if __name__ == "__main__":
    main()
