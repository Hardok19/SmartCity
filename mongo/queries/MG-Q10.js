// Parámetros por defecto: tipos_zona=comercial,industrial,mixta, desde=2024-03-01, hasta=2024-06-15, pm25_min=35.4, bateria_max=90, limite=200
// Uso: mongosh smartcity mongo/queries/MG-Q10.js
// MG-Q10 — Filtros múltiples
db.lectura.find({
  $and: [
    { tipo: "calidad_aire" },
    { "ubicacion.tipo_zona": { $in: ["comercial", "industrial", "mixta"] } },
    {
      timestamp: {
        $gte: ISODate("2024-03-01T00:00:00.000Z"),
        $lte: ISODate("2024-06-15T23:59:59.999Z")
      }
    },
    { "mediciones.co2": { $exists: true } },
    { $or: [{ "mediciones.pm25": { $gte: 35.4 } }, { "calidad_dato.valido": false }] },
    { "dispositivo.bateria_pct": { $lt: 90 } }
  ]
}, {
  _id: 0,
  sensor_id: 1,
  timestamp: 1,
  "ubicacion.zona": 1,
  "ubicacion.tipo_zona": 1,
  "mediciones.pm25": 1,
  "mediciones.co2": 1,
  dispositivo: 1,
  calidad_dato: 1
}).sort({ "mediciones.pm25": -1 }).limit(200)
