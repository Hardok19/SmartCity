#!/usr/bin/env python3
"""E8 — Empaqueta el código para la entrega: zip con los archivos versionados en git (sin data/raw, data/interim,
.venv ni .env), más el PDF del documento técnico si existe.

Usa `git ls-files`, así que solo entra lo que está en el repositorio (lo ignorado por .gitignore queda fuera) y
nunca un archivo de credenciales. Avisa si hay cambios sin commit, porque esos no se incluyen.

  python scripts/empaquetar_entrega.py             # -> dist/SmartCity_entrega_<fecha>.zip
"""
import subprocess
import zipfile
from datetime import date
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
EXCLUIR = (".env", ".venv/", "data/raw/", "data/interim/", "dist/")
EXTRA = [RAIZ / "docs" / "entrega" / "SmartCity_documento_tecnico.pdf"]


def git(*args):
    return subprocess.run(["git", *args], cwd=RAIZ, capture_output=True, text=True, check=True).stdout


def main():
    pendientes = git("status", "--porcelain").strip()
    if pendientes:
        print("AVISO: hay cambios sin commit; el zip solo incluye la versión del índice de git:")
        print("\n".join("   " + ln for ln in pendientes.splitlines()[:15]))
    archivos = [ln for ln in git("ls-files").splitlines()
                if ln and not any(ln == e.rstrip("/") or ln.startswith(e) for e in EXCLUIR)]
    destino = RAIZ / "dist" / f"SmartCity_entrega_{date.today():%Y%m%d}.zip"
    destino.parent.mkdir(exist_ok=True)
    with zipfile.ZipFile(destino, "w", zipfile.ZIP_DEFLATED) as z:
        for rel in archivos:
            ruta = RAIZ / rel
            if ruta.is_file():
                z.write(ruta, f"SmartCity/{rel}")
        for ruta in EXTRA:
            if ruta.exists() and ruta.relative_to(RAIZ).as_posix() not in archivos:
                z.write(ruta, f"SmartCity/{ruta.relative_to(RAIZ).as_posix()}")
    print(f"-> {destino.relative_to(RAIZ).as_posix()}: {len(archivos)} archivos, {destino.stat().st_size / 2**20:.1f} MB")
    print("Etiqueta sugerida tras el último commit:  git tag -a v1.0-entrega -m \"Entrega final\"")


if __name__ == "__main__":
    main()
