
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[1]
SQL_DIR = ROOT / "sql" / "ddl"


def inicializar_postgres():
    archivos = sorted(SQL_DIR.glob("*.sql"))

    for archivo in archivos:
        print(f"Ejecutando: {archivo.name}", flush=True)

        subprocess.run(
            [
                "docker", "exec", "-i",
                "smartcity_postgres",
                "psql", "-X",
                "-U", "smartcity",
                "-d", "smartcity_db",
                "-v", "ON_ERROR_STOP=1",
                "--single-transaction",
                "-f", "-"
            ],
            input=archivo.read_bytes(),
            check=True
        )

    print("Inicializacion completada correctamente.")


if __name__ == "__main__":
    inicializar_postgres()
