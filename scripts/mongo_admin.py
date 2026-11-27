#!/usr/bin/env python3
"""mongo_admin.py — B1.2. Versión Python (pymongo) de mongo/01_colecciones.js y mongo/02_indices.js.

Sirve en Windows/Linux sin mongosh ni montar el repo en el contenedor. Idempotente: se puede correr N veces.

  python scripts/mongo_admin.py colecciones   # crea/actualiza colecciones con validador $jsonSchema
  python scripts/mongo_admin.py indices       # crea los índices (createIndex es idempotente)
  python scripts/mongo_admin.py todo          # ambos, en ese orden

IMPORTANTE: los esquemas e índices de este archivo y de los .js deben mantenerse iguales.

Nombres de campos (decisión de B; el contrato.py NO cambia, el puente está en scripts/mongo_contrato.py):
  lectura.sensor_id          == id_sensor del contrato
  lectura.tipo               == categoría (calidad_aire | clima | ...); las variables viven en `mediciones`
  lectura.ubicacion.zona     == zona del contrato (string)        [lectura]
  <evento|alerta|incidente>.zona.nombre == zona del contrato      [las otras tres colecciones]
"""
import argparse
import os
import sys

from pymongo import ASCENDING, DESCENDING, GEOSPHERE, TEXT, MongoClient
from pymongo.errors import OperationFailure, ServerSelectionTimeoutError

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

URI_DEFECTO = "mongodb://smartcity:smartcity123@localhost:27017/smartcity_db?authSource=admin"
DB_DEFECTO = "smartcity_db"

# Tipos del contrato.py (TIPOS_METRICA) + "clima", que el contrato no conoce y se maneja aparte.
TIPOS_LECTURA = ["calidad_aire", "clima", "trafico", "energia", "ruido", "ocupacion_parqueo", "transporte_publico"]

ZONA = {"bsonType": "object", "required": ["id_zona", "nombre"],
        "properties": {"id_zona": {"bsonType": "int"}, "nombre": {"bsonType": "string"}}}

ESQUEMAS = {
    "lectura": {"bsonType": "object", "required": ["sensor_id", "tipo", "timestamp", "ubicacion"],
                "properties": {
                    "sensor_id": {"bsonType": "string"},
                    "tipo": {"enum": TIPOS_LECTURA},
                    "timestamp": {"bsonType": "date"},
                    "ubicacion": {"bsonType": "object", "required": ["id_zona", "zona"],
                                  "properties": {"id_zona": {"bsonType": "int"}, "zona": {"bsonType": "string"}}},
                    "mediciones": {"bsonType": "object"},   # sin propiedades fijas: cada tipo/firmware trae las suyas
                }},
    "evento": {"bsonType": "object", "required": ["tipo_evento", "severidad", "inicio", "zona"],
               "properties": {
                   "tipo_evento": {"enum": ["congestion", "pico_ruido", "contaminacion_alta", "evento_urbano",
                                            "falla_sensor", "lluvia_intensa", "ola_calor"]},
                   "severidad": {"enum": ["baja", "media", "alta", "critica"]},
                   "inicio": {"bsonType": "date"}, "zona": ZONA, "detalles": {"bsonType": "object"}}},
    "alerta": {"bsonType": "object", "required": ["regla", "variable", "valor", "umbral", "estado", "emitida", "zona"],
               "properties": {
                   "valor": {"bsonType": ["double", "int"]}, "umbral": {"bsonType": ["double", "int"]},
                   "estado": {"enum": ["abierta", "atendida", "cerrada"]}, "emitida": {"bsonType": "date"},
                   "zona": ZONA, "acciones": {"bsonType": "array"}}},
    "incidente": {"bsonType": "object",
                  "required": ["categoria", "titulo", "descripcion", "reportado", "zona", "estado_actual"],
                  "properties": {
                      "categoria": {"enum": ["accidente_transito", "bache", "semaforo_danado", "inundacion",
                                             "alumbrado", "residuos", "vandalismo", "quema_humo"]},
                      "titulo": {"bsonType": "string"}, "descripcion": {"bsonType": "string"},
                      "reportado": {"bsonType": "date"}, "zona": ZONA, "historial": {"bsonType": "array"}}},
}
COLECCIONES_SIN_VALIDADOR = ["lectura_tmp", "demo_crud"]


def crear_colecciones(db):
    existentes = set(db.list_collection_names())
    for nombre, esquema in ESQUEMAS.items():
        opciones = dict(validator={"$jsonSchema": esquema}, validationLevel="moderate", validationAction="error")
        if nombre in existentes:
            db.command("collMod", nombre, **opciones)
            print(f"  {nombre}: validador actualizado")
        else:
            db.create_collection(nombre, **opciones)
            print(f"  {nombre}: creada con validador")
    for nombre in COLECCIONES_SIN_VALIDADOR:
        if nombre not in existentes:
            db.create_collection(nombre)
            print(f"  {nombre}: creada")


def crear_indices(db):
    db["lectura"].create_index([("sensor_id", ASCENDING), ("timestamp", DESCENDING)], name="ix_sensor_ts")                 # MG-Q01
    db["lectura"].create_index([("tipo", ASCENDING), ("ubicacion.zona", ASCENDING), ("timestamp", DESCENDING)],
                               name="ix_tipo_zona_ts")                                                                    # MG-Q02/Q06/Q07
    db["lectura"].create_index([("timestamp", ASCENDING)], name="ix_ts")                                                  # rangos / export
    db["lectura"].create_index([("ubicacion.geo", GEOSPHERE)], name="ix_geo")                                             # $geoNear
    db["evento"].create_index([("tipo_evento", ASCENDING), ("severidad", ASCENDING), ("inicio", DESCENDING)],
                              name="ix_tipo_sev_inicio")                                                                  # MG-Q04
    db["evento"].create_index([("zona.geo", GEOSPHERE)], name="ix_geo")
    db["alerta"].create_index([("zona.nombre", ASCENDING), ("emitida", DESCENDING)], name="ix_zona_emitida")              # MG-Q05
    db["alerta"].create_index([("estado", ASCENDING)], name="ix_abiertas", partialFilterExpression={"estado": "abierta"})
    db["alerta"].create_index([("zona.geo", GEOSPHERE)], name="ix_geo")
    db["incidente"].create_index([("categoria", ASCENDING), ("reportado", DESCENDING)], name="ix_cat_reportado")          # MG-Q08
    db["incidente"].create_index([("titulo", TEXT), ("descripcion", TEXT)], name="ix_texto",
                                 weights={"titulo": 3, "descripcion": 1}, default_language="spanish")
    db["lectura_tmp"].create_index([("creado", ASCENDING)], name="ix_ttl", expireAfterSeconds=3600)                       # TTL
    for c in ["lectura", "evento", "alerta", "incidente", "lectura_tmp"]:
        print(f"  {c}: " + ", ".join(sorted(db[c].index_information())))


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("accion", choices=["colecciones", "indices", "todo"])
    ap.add_argument("--uri", default=os.environ.get("MONGO_URI", URI_DEFECTO))
    ap.add_argument("--db", default=os.environ.get("MONGO_DB", DB_DEFECTO))
    a = ap.parse_args()

    db = MongoClient(a.uri, serverSelectionTimeoutMS=5000)[a.db]
    try:
        db.command("ping")
        db.list_collection_names()
    except ServerSelectionTimeoutError:
        sys.exit("No se pudo conectar a MongoDB. ¿Está arriba el contenedor? (docker compose up -d mongo)")
    except OperationFailure as e:
        sys.exit(f"MongoDB rechazó las credenciales: {e.details.get('errmsg', e)}\n"
                 "Compose: usuario smartcity / contraseña smartcity123, authSource=admin. Revisa MONGO_URI o tu .env.")

    if a.accion in ("colecciones", "todo"):
        print("Colecciones:")
        crear_colecciones(db)
    if a.accion in ("indices", "todo"):
        print("Índices:")
        crear_indices(db)


if __name__ == "__main__":
    main()
