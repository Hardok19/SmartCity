"""Permite `pytest` desde la raíz sin instalar el proyecto: agrega la raíz del repo a sys.path."""
import sys
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
if str(RAIZ) not in sys.path:
    sys.path.insert(0, str(RAIZ))
