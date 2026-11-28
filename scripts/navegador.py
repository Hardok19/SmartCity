"""Utilidades de documentación: renderizar HTML a PNG o PDF con un navegador Chromium sin ventana.

Lo usan exportar_diagramas.py (Mermaid → PNG), graficos_benchmarks.py (Plotly → PNG) y ensamblar_documento.py
(Markdown → PDF). Busca Microsoft Edge o Google Chrome; otra ruta se indica con la variable NAVEGADOR.
Mermaid se descarga una sola vez a data/interim/herramientas/ (carpeta ignorada por git).
"""
from __future__ import annotations

import hashlib
import html
import os
import shutil
import subprocess
import tempfile
import time
import urllib.request
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
HERRAMIENTAS = RAIZ / "data" / "interim" / "herramientas"
MERMAID_URL = "https://cdn.jsdelivr.net/npm/mermaid@10.9.1/dist/mermaid.min.js"
CANDIDATOS = [
    r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
    r"C:\Program Files\Microsoft\Edge\Application\msedge.exe",
    r"C:\Program Files\Google\Chrome\Application\chrome.exe",
    "/usr/bin/google-chrome", "/usr/bin/chromium", "/usr/bin/chromium-browser", "/usr/bin/microsoft-edge",
]


def navegador() -> str:
    ruta = os.environ.get("NAVEGADOR")
    if ruta and Path(ruta).exists():
        return ruta
    for c in CANDIDATOS:
        if Path(c).exists():
            return c
    for nombre in ("msedge", "chrome", "chromium", "google-chrome"):
        if shutil.which(nombre):
            return shutil.which(nombre)
    raise FileNotFoundError("No se encontró Edge ni Chrome; defina NAVEGADOR con la ruta del ejecutable")


def _ejecutar(argumentos: list[str], salida: Path, timeout=120, intentos=3, espera_archivo=45):
    """Lanza el navegador y espera a que exista `salida`.

    En Windows msedge.exe puede devolver el control antes de terminar de escribir la captura o el PDF, por eso se
    sondea el archivo y, si no aparece, se reintenta con un perfil nuevo.
    """
    r = None
    for _ in range(intentos):
        salida.unlink(missing_ok=True)
        with tempfile.TemporaryDirectory(prefix="smartcity_nav_", ignore_cleanup_errors=True) as perfil:
            base = [navegador(), "--headless=new", "--disable-gpu", "--no-first-run", "--no-default-browser-check",
                    f"--user-data-dir={perfil}"]
            r = subprocess.run(base + argumentos, capture_output=True, text=True, timeout=timeout)
            limite = time.monotonic() + espera_archivo
            tam = -1
            while time.monotonic() < limite:
                if salida.exists() and salida.stat().st_size > 0 and salida.stat().st_size == tam:
                    return r
                tam = salida.stat().st_size if salida.exists() else -1
                time.sleep(0.5)
    return r


def html_a_png(pagina: Path, png: Path, ancho=2400, alto=2000, escala=2, espera_ms=6000, recortar=True, margen=24):
    """Captura la página y recorta el fondo blanco sobrante."""
    png.parent.mkdir(parents=True, exist_ok=True)
    r = _ejecutar([f"--window-size={ancho},{alto}", f"--force-device-scale-factor={escala}", "--hide-scrollbars",
                   f"--virtual-time-budget={espera_ms}", f"--screenshot={png}", pagina.resolve().as_uri()], png)
    if not png.exists():
        raise RuntimeError(f"El navegador no generó {png} (código {r.returncode}): {(r.stderr or r.stdout)[-1500:]}")
    if recortar:
        from PIL import Image, ImageChops
        img = Image.open(png).convert("RGB")
        fondo = Image.new("RGB", img.size, (255, 255, 255))
        caja = ImageChops.difference(img, fondo).getbbox()
        if caja:
            m = margen * escala
            caja = (max(caja[0] - m, 0), max(caja[1] - m, 0), min(caja[2] + m, img.width), min(caja[3] + m, img.height))
            img.crop(caja).save(png, optimize=True)
    return png


def html_a_pdf(pagina: Path, pdf: Path, espera_ms=15000):
    pdf.parent.mkdir(parents=True, exist_ok=True)
    r = _ejecutar([f"--virtual-time-budget={espera_ms}", "--no-pdf-header-footer", f"--print-to-pdf={pdf}",
                   pagina.resolve().as_uri()], pdf, timeout=300)
    if not pdf.exists():
        raise RuntimeError(f"El navegador no generó {pdf} (código {r.returncode}): {(r.stderr or r.stdout)[-1500:]}")
    return pdf


def mermaid_js() -> Path:
    destino = HERRAMIENTAS / "mermaid.min.js"
    if not destino.exists():
        HERRAMIENTAS.mkdir(parents=True, exist_ok=True)
        print(f"  descargando {MERMAID_URL}")
        with urllib.request.urlopen(MERMAID_URL, timeout=60) as r:
            destino.write_bytes(r.read())
    return destino


def mermaid_a_png(codigo: str, png: Path, ancho=4400) -> Path:
    """Renderiza un bloque Mermaid (el mismo texto que GitHub) a PNG."""
    pagina = HERRAMIENTAS / f"mermaid_{hashlib.sha1(codigo.encode()).hexdigest()[:12]}.html"
    pagina.parent.mkdir(parents=True, exist_ok=True)
    pagina.write_text(f"""<!doctype html><html><head><meta charset="utf-8">
<script src="{mermaid_js().as_uri()}"></script>
<style>body{{margin:0;background:#fff;font-family:Segoe UI,Arial,sans-serif}} #d{{display:inline-block;padding:20px}}</style>
</head><body><div id="d"><pre class="mermaid">{html.escape(codigo)}</pre></div>
<script>mermaid.initialize({{startOnLoad:true, theme:'default', securityLevel:'loose',
  flowchart:{{useMaxWidth:false, htmlLabels:true}}, er:{{useMaxWidth:false}}}});</script></body></html>""",
                      encoding="utf-8")
    return html_a_png(pagina, png, ancho=ancho, alto=3000)
