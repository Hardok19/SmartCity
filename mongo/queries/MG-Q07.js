// Parámetros por defecto: desde=2024-05-01, hasta=2024-05-31
// Uso: mongosh smartcity mongo/queries/MG-Q07.js
// MG-Q07 — Promedio por tipo de sensor
db.lectura.aggregate([
  {
    $match: {
      timestamp: {
        $gte: ISODate("2024-05-01T00:00:00.000Z"),
        $lte: ISODate("2024-05-31T23:59:59.999Z")
      }
    }
  },
  { $project: { tipo: 1, sensor_id: 1, kv: { $objectToArray: "$mediciones" } } },
  { $unwind: "$kv" },
  { $match: { "kv.v": { $type: "number" } } },
  {
    $group: {
      _id: { tipo: "$tipo", variable: "$kv.k" },
      promedio: { $avg: "$kv.v" },
      desviacion: { $stdDevPop: "$kv.v" },
      sensores: { $addToSet: "$sensor_id" }
    }
  },
  {
    $project: {
      _id: 0,
      tipo: "$_id.tipo",
      variable: "$_id.variable",
      promedio: { $round: ["$promedio", 2] },
      desviacion: { $round: ["$desviacion", 2] },
      sensores: { $size: "$sensores" }
    }
  },
  { $sort: { tipo: 1, variable: 1 } }
], { allowDiskUse: true })
