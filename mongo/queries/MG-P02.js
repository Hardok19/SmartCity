// Parámetros por defecto: variable=pm25, umbral=35.4, desde=2024-02-15, hasta=2024-06-15
// Uso: mongosh smartcity mongo/queries/MG-P02.js
// MG-P02 — ¿Qué sensores registraron valores superiores al límite establecido?
db.lectura.aggregate([
  {
    $match: {
      tipo: "calidad_aire",
      "mediciones.pm25": { $gt: 35.4 },
      timestamp: {
        $gte: ISODate("2024-02-15T00:00:00.000Z"),
        $lte: ISODate("2024-06-15T23:59:59.999Z")
      }
    }
  },
  {
    $group: {
      _id: "$sensor_id",
      zona: { $first: "$ubicacion.zona" },
      lecturas_sobre_limite: { $sum: 1 },
      valor_maximo: { $max: "$mediciones.pm25" },
      primera: { $min: "$timestamp" },
      ultima: { $max: "$timestamp" }
    }
  },
  {
    $project: {
      _id: 0,
      sensor_id: "$_id",
      zona: 1,
      lecturas_sobre_limite: 1,
      valor_maximo: 1,
      primera: 1,
      ultima: 1,
      umbral: { $literal: 35.4 }
    }
  },
  { $sort: { lecturas_sobre_limite: -1 } }
], { allowDiskUse: true })
