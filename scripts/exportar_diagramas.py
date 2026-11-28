#!/usr/bin/env python3
"""E2 — Exporta a PNG cada diagrama Mermaid de docs/diagramas/*.md (y el flujo de 08_procesamiento.md).

El PNG se llama como el .md (si un archivo tiene varios bloques: <nombre>_2.png, …) y queda en docs/diagramas/img/.
Requiere Edge o Chrome (ver scripts/navegador.py).

  python scripts/exportar_diagramas.py
"""
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from navegador import RAIZ, mermaid_a_png  # noqa: E402

DIAGRAMAS = RAIZ / "docs" / "diagramas"
EXTRA = {RAIZ / "docs" / "tecnico" / "08_procesamiento.md": "flujo_spark"}
BLOQUE = re.compile(r"```mermaid\n(.*?)```", re.S)


def main():
    fuentes = {p: p.stem for p in sorted(DIAGRAMAS.glob("*.md"))} | EXTRA
    for md, nombre in fuentes.items():
        bloques = BLOQUE.findall(md.read_text(encoding="utf-8"))
        for i, codigo in enumerate(bloques, start=1):
            png = DIAGRAMAS / "img" / (f"{nombre}.png" if i == 1 else f"{nombre}_{i}.png")
            mermaid_a_png(codigo, png)
            print(f"-> {png.relative_to(RAIZ).as_posix()}")


if __name__ == "__main__":
    main()
