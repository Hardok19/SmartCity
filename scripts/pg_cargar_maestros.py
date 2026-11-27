"""A1.5 — Carga los datos maestros de PostgreSQL (idempotente: se puede ejecutar varias veces).

Fuentes: config/zonas.yaml y data/maestros/sensores.csv (el mismo catálogo que usa la pareja B).
Carga:
  * ubicaciones (5 zonas, con distrito y centroide)
  * organismos (los del catálogo + 2 sin sensores, para mostrar el LEFT JOIN de PG-Q04), con responsable ficticio
  * sensores del catálogo (40) + contadores de tráfico T-001…T-005 y medidores de energía E-001…E-005 (D-03)
  * usuarios ficticios (40, correos @smartcity.example), semilla 2026
Categorías, tipos de sensor, variables y calendario se crean en los DDL (sql/ddl/001, 002, 006).

    python scripts/pg_init.py              # primero el esquema
    python scripts/pg_cargar_maestros.py
"""
import csv
import random
import sys
from pathlib import Path

import psycopg2
import yaml

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ))
from comun import CONFIG  # noqa: E402

SEMILLA = 2026
ORGANISMOS_SIN_SENSORES = ["Oficina Municipal de Datos Urbanos", "Unidad de Gestión de Emergencias"]
ORGANISMO_TRAFICO = "Transport for London"
ORGANISMO_ENERGIA = "Greater London Authority"
NOMBRES = ["Ana", "Luis", "María", "Carlos", "Sofía", "Diego", "Valeria", "Andrés", "Camila", "José",
           "Lucía", "Mateo", "Paula", "Daniel", "Elena", "Gabriel", "Isabel", "Tomás", "Laura", "Martín"]
APELLIDOS = ["Rojas", "Vargas", "Mora", "Jiménez", "Castro", "Solís", "Araya", "Chaves", "Brenes", "Quesada",
             "Alfaro", "Campos", "Salas", "Mena", "Herrera", "Navarro", "Ulate", "Pérez", "Calderón", "Soto"]


def _correo(nombre: str, apellido: str, n: int) -> str:
    tabla = str.maketrans("áéíóúñÁÉÍÓÚÑ", "aeiounAEIOUN")
    return f"{nombre.translate(tabla).lower()}.{apellido.translate(tabla).lower()}{n:02d}@smartcity.example"


def main():
    zonas = yaml.safe_load((RAIZ / "config/zonas.yaml").read_text(encoding="utf-8"))["zonas"]
    ruta = RAIZ / "data/maestros/sensores.csv"
    if not ruta.exists():
        ruta = RAIZ / "data/sensores.csv"
    filas = list(csv.DictReader(ruta.open(encoding="utf-8-sig", newline="")))
    rng = random.Random(SEMILLA)

    con = psycopg2.connect(host=CONFIG.pg_host, port=CONFIG.pg_port, dbname=CONFIG.pg_db,
                           user=CONFIG.pg_user, password=CONFIG.pg_password)
    with con, con.cursor() as cur:
        # --- ubicaciones
        for z in zonas:
            cur.execute("""INSERT INTO ubicaciones (nombre_zona, distrito, latitud, longitud) VALUES (%s, %s, %s, %s)
                           ON CONFLICT (nombre_zona) DO UPDATE SET distrito = EXCLUDED.distrito,
                               latitud = EXCLUDED.latitud, longitud = EXCLUDED.longitud""",
                        (z["zona"], z["distrito"], z["lat"], z["lon"]))

        # --- organismos (responsable ficticio, determinista)
        organismos = sorted({f["organismo"] for f in filas} | {ORGANISMO_TRAFICO, ORGANISMO_ENERGIA}
                            | set(ORGANISMOS_SIN_SENSORES))
        for nombre in organismos:
            responsable = f"{rng.choice(NOMBRES)} {rng.choice(APELLIDOS)}"
            cur.execute("""INSERT INTO organismos (nombre, responsable) VALUES (%s, %s)
                           ON CONFLICT (nombre) DO UPDATE SET responsable = EXCLUDED.responsable""",
                        (nombre, responsable))

        # --- sensores del catálogo + contadores/medidores de D-03
        sensores = [(f["id_sensor"], f["nombre"], f["tipo_metrica"], f["fecha_instalacion"] or None, f["estado"],
                     f["zona"], f["organismo"], f["origen"], f["envia_datos"].lower() == "true",
                     f["fecha_baja"] or None, f["id_estacion_openaq"] or None, int(f["frecuencia_min"] or 60),
                     float(f["lat"]) if f["lat"] else None, float(f["lon"]) if f["lon"] else None)
                    for f in filas]
        for z in zonas:
            sensores.append((f"T-{z['id_zona']:03d}", f"Contador vehicular {z['distrito']}", "trafico", "2022-12-01",
                             "activo", z["zona"], ORGANISMO_TRAFICO, "sintetico", True, None, None, 60, None, None))
            sensores.append((f"E-{z['id_zona']:03d}", f"Medidor de energía {z['distrito']}", "energia", "2022-12-01",
                             "activo", z["zona"], ORGANISMO_ENERGIA, "sintetico", True, None, None, 60, None, None))
        for s in sensores:
            cur.execute("""
                INSERT INTO sensores (id_sensor, nombre, tipo_sensor, fecha_instalacion, estado, id_ubicacion,
                                      id_organismo, origen, envia_datos, fecha_baja, id_estacion_openaq, frecuencia_min,
                                      latitud, longitud)
                VALUES (%s, %s, %s, %s, %s,
                        (SELECT id_ubicacion FROM ubicaciones WHERE nombre_zona = %s),
                        (SELECT id_organismo FROM organismos WHERE nombre = %s),
                        %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id_sensor) DO UPDATE SET
                    nombre = EXCLUDED.nombre, tipo_sensor = EXCLUDED.tipo_sensor,
                    fecha_instalacion = EXCLUDED.fecha_instalacion, estado = EXCLUDED.estado,
                    id_ubicacion = EXCLUDED.id_ubicacion, id_organismo = EXCLUDED.id_organismo,
                    origen = EXCLUDED.origen, envia_datos = EXCLUDED.envia_datos, fecha_baja = EXCLUDED.fecha_baja,
                    id_estacion_openaq = EXCLUDED.id_estacion_openaq, frecuencia_min = EXCLUDED.frecuencia_min,
                    latitud = EXCLUDED.latitud, longitud = EXCLUDED.longitud""", s)

        # --- usuarios ficticios
        cur.execute("SELECT id_organismo FROM organismos ORDER BY id_organismo")
        ids_org = [r[0] for r in cur.fetchall()]
        for n in range(1, 41):
            nombre, apellido = rng.choice(NOMBRES), rng.choice(APELLIDOS)
            cur.execute("""INSERT INTO usuarios (nombre, correo, id_organismo) VALUES (%s, %s, %s)
                           ON CONFLICT (correo) DO UPDATE SET nombre = EXCLUDED.nombre,
                               id_organismo = EXCLUDED.id_organismo""",
                        (f"{nombre} {apellido}", _correo(nombre, apellido, n), rng.choice(ids_org)))

        for tabla in ("ubicaciones", "categorias_sensor", "tipos_sensor", "organismos", "sensores", "usuarios",
                      "variables_medicion", "calendario"):
            cur.execute(f"SELECT COUNT(*) FROM {tabla}")
            print(f"{tabla:20s} {cur.fetchone()[0]:>6}")
        cur.execute("""SELECT COUNT(*) FROM sensores s LEFT JOIN ubicaciones u USING (id_ubicacion)
                       LEFT JOIN organismos o USING (id_organismo) LEFT JOIN tipos_sensor t USING (tipo_sensor)
                       WHERE u.id_ubicacion IS NULL OR o.id_organismo IS NULL OR t.tipo_sensor IS NULL""")
        huerfanos = cur.fetchone()[0]
        print("FK válidas:", "OK" if huerfanos == 0 else f"{huerfanos} sensores con referencias faltantes")
    con.close()


if __name__ == "__main__":
    main()
