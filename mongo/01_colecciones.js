// 01_colecciones.js — B1.2. Colecciones con validador $jsonSchema (validationLevel: moderate).
// Equivalente exacto de `python scripts/mongo_admin.py colecciones` (mantener ambos iguales).
// Uso: docker exec -i smartcity_mongo mongosh -u smartcity -p smartcity123 --authenticationDatabase admin smartcity_db < mongo/01_colecciones.js
// Idempotente: si la colección existe, actualiza el validador con collMod.

const TIPOS_LECTURA = ["calidad_aire", "clima", "trafico", "energia", "ruido", "ocupacion_parqueo", "transporte_publico"];
const ZONA = {
  bsonType: "object", required: ["id_zona", "nombre"],
  properties: { id_zona: { bsonType: "int" }, nombre: { bsonType: "string" } }
};

const ESQUEMAS = {
  lectura: {
    bsonType: "object", required: ["sensor_id", "tipo", "timestamp", "ubicacion"],
    properties: {
      sensor_id: { bsonType: "string" },
      tipo: { enum: TIPOS_LECTURA },
      timestamp: { bsonType: "date" },
      ubicacion: {
        bsonType: "object", required: ["id_zona", "zona"],
        properties: { id_zona: { bsonType: "int" }, zona: { bsonType: "string" } }
      },
      mediciones: { bsonType: "object" }   // sin propiedades fijas: cada tipo/firmware trae las suyas
    }
  },
  evento: {
    bsonType: "object", required: ["tipo_evento", "severidad", "inicio", "zona"],
    properties: {
      tipo_evento: { enum: ["congestion", "pico_ruido", "contaminacion_alta", "evento_urbano",
                            "falla_sensor", "lluvia_intensa", "ola_calor"] },
      severidad: { enum: ["baja", "media", "alta", "critica"] },
      inicio: { bsonType: "date" }, zona: ZONA, detalles: { bsonType: "object" }
    }
  },
  alerta: {
    bsonType: "object", required: ["regla", "variable", "valor", "umbral", "estado", "emitida", "zona"],
    properties: {
      valor: { bsonType: ["double", "int"] }, umbral: { bsonType: ["double", "int"] },
      estado: { enum: ["abierta", "atendida", "cerrada"] }, emitida: { bsonType: "date" },
      zona: ZONA, acciones: { bsonType: "array" }
    }
  },
  incidente: {
    bsonType: "object", required: ["categoria", "titulo", "descripcion", "reportado", "zona", "estado_actual"],
    properties: {
      categoria: { enum: ["accidente_transito", "bache", "semaforo_danado", "inundacion",
                          "alumbrado", "residuos", "vandalismo", "quema_humo"] },
      titulo: { bsonType: "string" }, descripcion: { bsonType: "string" },
      reportado: { bsonType: "date" }, zona: ZONA, historial: { bsonType: "array" }
    }
  }
};
const SIN_VALIDADOR = ["lectura_tmp", "demo_crud"];

const existentes = db.getCollectionNames();
for (const [nombre, esquema] of Object.entries(ESQUEMAS)) {
  const opciones = { validator: { $jsonSchema: esquema }, validationLevel: "moderate", validationAction: "error" };
  if (existentes.includes(nombre)) {
    db.runCommand({ collMod: nombre, ...opciones });
    print(`  ${nombre}: validador actualizado`);
  } else {
    db.createCollection(nombre, opciones);
    print(`  ${nombre}: creada con validador`);
  }
}
for (const nombre of SIN_VALIDADOR) {
  if (!existentes.includes(nombre)) { db.createCollection(nombre); print(`  ${nombre}: creada`); }
}

// Aceptación B1.2: un documento sin sensor_id debe fallar.
try {
  db.lectura.insertOne({ tipo: "clima", timestamp: new Date(), ubicacion: { id_zona: 1, zona: "Zona Norte" } });
  print("ERROR: se aceptó una lectura sin sensor_id");
} catch (e) {
  print("OK: lectura sin sensor_id rechazada por el validador (" + (e.codeName || e.code || e.message) + ")");
}
