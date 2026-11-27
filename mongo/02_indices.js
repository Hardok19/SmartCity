// 02_indices.js — B1.2. Índices de las colecciones (createIndex es idempotente).
// Equivalente exacto de `python scripts/mongo_admin.py indices` (mantener ambos iguales).
// Uso: docker exec -i smartcity_mongo mongosh -u smartcity -p smartcity123 --authenticationDatabase admin smartcity_db < mongo/02_indices.js

db.lectura.createIndex({ sensor_id: 1, timestamp: -1 }, { name: "ix_sensor_ts" });                          // MG-Q01
db.lectura.createIndex({ tipo: 1, "ubicacion.zona": 1, timestamp: -1 }, { name: "ix_tipo_zona_ts" });       // MG-Q02/Q06/Q07
db.lectura.createIndex({ timestamp: 1 }, { name: "ix_ts" });                                                 // rangos / export
db.lectura.createIndex({ "ubicacion.geo": "2dsphere" }, { name: "ix_geo" });                                 // $geoNear
db.evento.createIndex({ tipo_evento: 1, severidad: 1, inicio: -1 }, { name: "ix_tipo_sev_inicio" });         // MG-Q04
db.evento.createIndex({ "zona.geo": "2dsphere" }, { name: "ix_geo" });
db.alerta.createIndex({ "zona.nombre": 1, emitida: -1 }, { name: "ix_zona_emitida" });                       // MG-Q05
db.alerta.createIndex({ estado: 1 }, { name: "ix_abiertas", partialFilterExpression: { estado: "abierta" } });
db.alerta.createIndex({ "zona.geo": "2dsphere" }, { name: "ix_geo" });
db.incidente.createIndex({ categoria: 1, reportado: -1 }, { name: "ix_cat_reportado" });                     // MG-Q08
db.incidente.createIndex({ titulo: "text", descripcion: "text" },
  { name: "ix_texto", weights: { titulo: 3, descripcion: 1 }, default_language: "spanish" });
db.lectura_tmp.createIndex({ creado: 1 }, { name: "ix_ttl", expireAfterSeconds: 3600 });                     // TTL

// Aceptación B1.2: getIndexes() los lista.
for (const c of ["lectura", "evento", "alerta", "incidente", "lectura_tmp"]) {
  print(`  ${c}: ` + db.getCollection(c).getIndexes().map(i => i.name).sort().join(", "));
}
