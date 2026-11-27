// 03_crud_demo.js — demostración CRUD (REQ-MG-CRUD) sobre demo_crud. Espejo de smartcity.queries.mongo_crud.
db.demo_crud.deleteMany({});
const ubic = (id, zona, lon, lat) => ({ id_zona: id, zona: zona, geo: { type: "Point", coordinates: [lon, lat] } });

print("insertOne");
printjson(db.demo_crud.insertOne({ sensor_id: "S-DEMO-001", tipo: "calidad_aire", timestamp: ISODate("2024-06-01T08:00:00Z"),
  ubicacion: ubic(1, "Zona Norte", -0.1058, 51.5465), mediciones: { pm25: 18.6, pm10: 25.1, co: 0.85, no2: 12.3 } }));

print("insertMany");
printjson(db.demo_crud.insertMany([
  { sensor_id: "S-DEMO-002", tipo: "clima", timestamp: ISODate("2024-06-01T09:00:00Z"), ubicacion: ubic(5, "Zona Oeste", -0.3037, 51.4613),
    mediciones: { temperatura: 27.2, humedad: 70 } },
  { sensor_id: "S-DEMO-003", tipo: "ruido", timestamp: ISODate("2024-06-01T09:00:00Z"), ubicacion: ubic(3, "Zona Centro", -0.1357, 51.4975),
    mediciones: { db_promedio: 142.0, db_max: 150.0 } },
  { sensor_id: "S-DEMO-004", tipo: "calidad_aire", timestamp: ISODate("2024-06-01T10:00:00Z"), ubicacion: ubic(2, "Zona Sur", -0.1949, 51.4098),
    mediciones: { pm25: -4.0, co2: 520 } },
  { sensor_id: "S-DEMO-005", tipo: "flujo_peatonal", timestamp: ISODate("2024-06-01T10:00:00Z"), ubicacion: ubic(4, "Zona Este", 0.0648, 51.4892),
    mediciones: { entradas: 85, salidas: 60, direccion_predominante: "entrada" } },
  { tipo_documento: "alerta", regla: "pm25_nivel_1", variable: "pm25", valor: 41.8, umbral: 35.4, sensor_id: "S-DEMO-001",
    estado: "abierta", emitida: ISODate("2024-06-01T11:00:00Z"), acciones: [] }
]));

print("find");
printjson(db.demo_crud.find({ tipo: { $in: ["calidad_aire", "clima"] } }, { _id: 0, sensor_id: 1, tipo: 1, mediciones: 1 })
  .sort({ timestamp: -1 }).limit(5).toArray());

print("findOne");
printjson(db.demo_crud.findOne({ sensor_id: "S-DEMO-001", tipo: "calidad_aire" }, { _id: 0 }));

print("updateOne");
printjson(db.demo_crud.updateOne({ tipo_documento: "alerta", estado: "abierta" },
  { $set: { estado: "atendida" }, $push: { acciones: { fecha: new Date(), usuario: "operador@smartcity.example", accion: "Inspección" } } }));

print("updateMany");
printjson(db.demo_crud.updateMany({ $or: [{ "mediciones.pm25": { $lt: 0 } }, { "mediciones.db_promedio": { $gt: 140 } }] },
  { $set: { "calidad_dato.valido": false }, $addToSet: { "calidad_dato.flags": "fuera_de_rango" } }));

print("deleteOne");
printjson(db.demo_crud.deleteOne({ sensor_id: "S-DEMO-005" }));
