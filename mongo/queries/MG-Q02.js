// Parámetros por defecto: desde=2024-04-01, hasta=2024-04-01, zona=Zona Norte, tipo=(todas), limite=500
// Uso: mongosh smartcity mongo/queries/MG-Q02.js
// MG-Q02 — Lecturas en un rango de fechas
db.lectura.find({
  "ubicacion.zona": "Zona Norte",
  timestamp: {
    $gte: ISODate("2024-04-01T00:00:00.000Z"),
    $lte: ISODate("2024-04-01T23:59:59.999Z")
  }
}, { _id: 0, sensor_id: 1, tipo: 1, timestamp: 1, "ubicacion.zona": 1, mediciones: 1 }).sort({ timestamp: 1 }).limit(500)
