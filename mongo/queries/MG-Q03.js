// Parámetros por defecto: desde=2024-04-01, hasta=2024-04-30
// Uso: mongosh smartcity mongo/queries/MG-Q03.js
// MG-Q03 — Valores máximos y mínimos registrados
db.lectura.aggregate([
  {
    $match: {
      timestamp: {
        $gte: ISODate("2024-04-01T00:00:00.000Z"),
        $lte: ISODate("2024-04-30T23:59:59.999Z")
      }
    }
  },
  { $project: { tipo: 1, kv: { $objectToArray: "$mediciones" } } },
  { $unwind: "$kv" },
  { $match: { "kv.v": { $type: "number" } } },
  {
    $group: {
      _id: { tipo: "$tipo", variable: "$kv.k" },
      minimo: { $min: "$kv.v" },
      maximo: { $max: "$kv.v" },
      promedio: { $avg: "$kv.v" },
      lecturas: { $sum: 1 }
    }
  },
  {
    $project: {
      _id: 0,
      tipo: "$_id.tipo",
      variable: "$_id.variable",
      minimo: 1,
      maximo: 1,
      promedio: { $round: ["$promedio", 2] },
      lecturas: 1
    }
  },
  { $sort: { tipo: 1, variable: 1 } }
], { allowDiskUse: true })
