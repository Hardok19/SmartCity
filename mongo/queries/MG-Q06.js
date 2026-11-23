// Parámetros por defecto: tipo=calidad_aire, desde=2024-02-15, hasta=2024-06-15
// Uso: mongosh smartcity mongo/queries/MG-Q06.js
// MG-Q06 — Promedio de mediciones por zona
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
      lecturas: { $sum: 1 },
      pm25: { $avg: "$mediciones.pm25" },
      pm10: { $avg: "$mediciones.pm10" },
      co: { $avg: "$mediciones.co" },
      no2: { $avg: "$mediciones.no2" },
      o3: { $avg: "$mediciones.o3" },
      co2: { $avg: "$mediciones.co2" }
    }
  },
  {
    $project: {
      _id: 0,
      zona: "$_id",
      lecturas: 1,
      pm25: { $round: ["$pm25", 2] },
      pm10: { $round: ["$pm10", 2] },
      co: { $round: ["$co", 2] },
      no2: { $round: ["$no2", 2] },
      o3: { $round: ["$o3", 2] },
      co2: { $round: ["$co2", 2] }
    }
  },
  { $sort: { pm25: -1 } }
], { allowDiskUse: true })
