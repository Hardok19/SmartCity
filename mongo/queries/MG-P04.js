// Parámetros por defecto: desde=2024-02-15, hasta=2024-06-15
// Uso: mongosh smartcity mongo/queries/MG-P04.js
// MG-P04 — ¿Qué zonas presentan mayor concentración de contaminación?
db.lectura.aggregate([
  {
    $match: {
      tipo: "calidad_aire",
      timestamp: {
        $gte: ISODate("2024-02-15T00:00:00.000Z"),
        $lte: ISODate("2024-06-15T23:59:59.999Z")
      }
    }
  },
  {
    $group: {
      _id: "$ubicacion.zona",
      pm25: { $avg: "$mediciones.pm25" },
      pm10: { $avg: "$mediciones.pm10" },
      no2: { $avg: "$mediciones.no2" },
      sobre_umbral: { $sum: { $cond: [{ $gt: ["$mediciones.pm25", 35.4] }, 1, 0] } },
      lecturas: { $sum: 1 }
    }
  },
  {
    $project: {
      _id: 0,
      zona: "$_id",
      pm25: { $round: ["$pm25", 2] },
      pm10: { $round: ["$pm10", 2] },
      no2: { $round: ["$no2", 2] },
      pct_sobre_umbral: { $round: [{ $multiply: [{ $divide: ["$sobre_umbral", "$lecturas"] }, 100] }, 2] }
    }
  },
  { $sort: { pm25: -1 } }
], { allowDiskUse: true })
