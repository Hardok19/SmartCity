#!/usr/bin/env python3
"""E1 — Ensambla el documento técnico (docs/tecnico/01…10) en un solo HTML y lo imprime a PDF.

* Los bloques Mermaid se reemplazan por su PNG (se renderiza con scripts/navegador.py y se guarda en caché).
* Las imágenes con ruta relativa se resuelven respecto del .md que las contiene.
* Agrega portada, índice y, como anexos, la matriz de resultados (benchmarks.md).
Requiere el paquete `markdown` y Edge o Chrome.

  python scripts/ensamblar_documento.py            # -> docs/entrega/SmartCity_documento_tecnico.pdf (+ .html)
"""
from __future__ import annotations

import hashlib
import re
import sys
from datetime import date
from pathlib import Path

import markdown

sys.path.insert(0, str(Path(__file__).resolve().parent))
from navegador import HERRAMIENTAS, RAIZ, html_a_pdf, mermaid_a_png  # noqa: E402

TECNICO = RAIZ / "docs" / "tecnico"
SECCIONES = ["01_descripcion_problema", "02_fuentes_datos", "03_arquitectura", "04_justificacion_tecnologica",
             "05_modelo_relacional", "06_modelo_documental", "07_estrategia_big_data", "08_procesamiento",
             "09_resultados", "10_conclusiones"]
ANEXOS = [RAIZ / "docs" / "diagramas" / "er_postgres.md", RAIZ / "docs" / "diagramas" / "modelo_documental.md",
          RAIZ / "docs" / "resultados" / "benchmarks.md"]
SALIDA = RAIZ / "docs" / "entrega"
BLOQUE = re.compile(r"```mermaid\n(.*?)```", re.S)
IMAGEN = re.compile(r"!\[([^\]]*)\]\(([^)]+)\)")
# Marcadores invisibles en GitHub: el PDF incrusta el primer diagrama Mermaid de otro .md, o una imagen si existe.
INCLUIR_DIAGRAMA = re.compile(r"<!--\s*incluir-diagrama:\s*(\S+)\s*-->")
INCLUIR_IMAGEN = re.compile(r"<!--\s*incluir-imagen:\s*(\S+)\s*-->")

ESTILO = """
@page { size: A4; margin: 18mm 16mm; }
body { font-family: 'Segoe UI', Arial, sans-serif; font-size: 10.5pt; line-height: 1.45; color: #1d1d1f; }
h1 { font-size: 19pt; border-bottom: 2px solid #336791; padding-bottom: 4px; margin-top: 0; page-break-before: always; }
h1.portada { page-break-before: avoid; border: none; font-size: 28pt; margin-top: 60mm; }
h2 { font-size: 14pt; color: #233d5c; margin-top: 18px; } h3 { font-size: 12pt; }
table { border-collapse: collapse; width: 100%; margin: 8px 0; font-size: 9pt; page-break-inside: auto; }
th, td { border: 1px solid #c9ced6; padding: 3px 6px; vertical-align: top; text-align: left; }
th { background: #eef2f7; } tr { page-break-inside: avoid; }
code { font-family: Consolas, monospace; font-size: 8.8pt; background: #f3f4f6; padding: 0 2px; }
pre { background: #f6f8fa; border: 1px solid #e1e4e8; padding: 6px; font-size: 8pt; white-space: pre-wrap; }
pre code { background: none; }
img { max-width: 100%; max-height: 210mm; display: block; margin: 8px auto; }
blockquote { border-left: 3px solid #c9ced6; margin: 6px 0; padding-left: 10px; color: #555; }
.indice { list-style: none; padding-left: 0; } .indice li { margin: 2px 0; } .meta { color: #555; margin-top: 8mm; }
"""


def png_mermaid(codigo: str) -> Path:
    png = HERRAMIENTAS / "doc" / f"{hashlib.sha1(codigo.encode()).hexdigest()[:16]}.png"
    if not png.exists():
        mermaid_a_png(codigo, png)
    return png


def preparar(md: Path) -> str:
    texto = md.read_text(encoding="utf-8-sig")

    def diagrama(m):
        origen = (md.parent / m.group(1)).resolve()
        bloques = BLOQUE.findall(origen.read_text(encoding="utf-8")) if origen.exists() else []
        return f"```mermaid\n{bloques[0]}```" if bloques else ""

    def imagen_opcional(m):
        destino = (md.parent / m.group(1)).resolve()
        return f"![]({destino.as_uri()})" if destino.exists() else ""
    texto = INCLUIR_DIAGRAMA.sub(diagrama, texto)
    texto = INCLUIR_IMAGEN.sub(imagen_opcional, texto)
    texto = BLOQUE.sub(lambda m: f"![diagrama]({png_mermaid(m.group(1)).as_uri()})", texto)

    def imagen(m):
        ruta = m.group(2)
        if ruta.startswith(("http", "file:")):
            return m.group(0)
        destino = (md.parent / ruta).resolve()
        return f"![{m.group(1)}]({destino.as_uri()})" if destino.exists() else f"*[{m.group(1)}: imagen pendiente]*"
    texto = IMAGEN.sub(imagen, texto)
    # los vínculos a otros .md no sirven en el PDF: se dejan como texto con su ruta
    texto = re.sub(r"(?<!!)\[([^\]]+)\]\(((?!http)[^)]+\.(?:md|csv|py|sql|js|sh|ps1|yml|yaml|txt))\)", r"\1 (`\2`)", texto)
    return texto


def codigo_en_linea(texto: str) -> str:
    """`x` del título Markdown -> <code>x</code> para el índice."""
    return re.sub(r"`([^`]*)`", lambda m: f"<code>{m.group(1)}</code>", texto)


def main():
    SALIDA.mkdir(parents=True, exist_ok=True)
    conversor = markdown.Markdown(extensions=["tables", "fenced_code", "sane_lists", "attr_list"])
    partes, indice = [], []
    for nombre in SECCIONES:
        md = TECNICO / f"{nombre}.md"
        if not md.exists():
            print(f"  falta {md.name}: se omite")
            continue
        texto = preparar(md)
        titulo = next((ln[2:] for ln in texto.splitlines() if ln.startswith("# ")), nombre)
        indice.append(titulo)
        partes.append(conversor.reset().convert(texto))
    for md in ANEXOS:
        if md.exists():
            texto = preparar(md)
            titulo = next((ln[2:] for ln in texto.splitlines() if ln.startswith("# ")), md.stem)
            texto = texto.replace(f"# {titulo}", f"# Anexo — {titulo}", 1)
            indice.append(f"Anexo — {titulo}")
            partes.append(conversor.reset().convert(texto))
    portada = (f"<h1 class='portada'>SmartCity Analytics</h1><h2>Documento técnico</h2>"
               f"<p>CE-3191 Bases de Datos · Instituto Tecnológico de Costa Rica · II semestre 2026</p>"
               f"<p class='meta'>Arquitectura con PostgreSQL, MongoDB, Apache Hadoop y Apache Spark.<br>"
               f"Versión generada el {date.today():%d/%m/%Y} desde <code>docs/tecnico/</code>.</p>"
               "<h2 style='margin-top:20mm'>Contenido</h2><ul class='indice'>"
               + "".join(f"<li>{codigo_en_linea(t)}</li>" for t in indice) + "</ul>")
    html = (f"<!doctype html><html lang='es'><head><meta charset='utf-8'><title>SmartCity — Documento técnico</title>"
            f"<style>{ESTILO}</style></head><body>{portada}{''.join(partes)}</body></html>")
    pagina = SALIDA / "SmartCity_documento_tecnico.html"
    pagina.write_text(html, encoding="utf-8")
    pdf = html_a_pdf(pagina, SALIDA / "SmartCity_documento_tecnico.pdf")
    print(f"-> {pdf.relative_to(RAIZ).as_posix()} ({pdf.stat().st_size / 2**20:.1f} MB)")


if __name__ == "__main__":
    main()
