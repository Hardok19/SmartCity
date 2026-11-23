// Parámetros por defecto: desde=2024-02-15, hasta=2024-06-15, radio_km=0.0, lat=51.5074, lon=-0.1278
// Uso: mongosh smartcity mongo/queries/MG-Q05.js
// MG-Q05 — Alertas emitidas por zona geográfica
db.alerta.aggregate([
  {
    $match: {
      emitida: {
        $gte: ISODate("2024-02-15T00:00:00.000Z"),
        $lte: ISODate("2024-06-15T23:59:59.999Z")
      }
    }
  },
  {
    $group: {
      _id: "$zona.nombre",
      alertas: { $sum: 1 },
      abiertas: { $sum: { $cond: [{ $eq: ["$estado", "abierta"] }, 1, 0] } },
      criticas_o_altas: { $sum: { $cond: [{ $in: ["$nivel", ["alta", "critica"]] }, 1, 0] } },
      variables: { $addToSet: "$variable" },
      ultima: { $max: "$emitida" }
    }
  },
  {
    $project: {
      _id: 0,
      zona: "$_id",
      alertas: 1,
      abiertas: 1,
      criticas_o_altas: 1,
      variables: 1,
      ultima: 1
    }
  },
  { $sort: { alertas: -1 } }
], { allowDiskUse: true })
