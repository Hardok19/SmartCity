// Parámetros por defecto: desde=2024-02-15, hasta=2024-06-15
// Uso: mongosh smartcity mongo/queries/MG-P03.js
// MG-P03 — ¿Cuántas mediciones produjo cada sensor?
db.lectura.aggregate([
  {
    $match: {
      timestamp: {
        $gte: ISODate("2024-02-15T00:00:00.000Z"),
        $lte: ISODate("2024-06-15T23:59:59.999Z")
      }
    }
  },
  {
    $group: {
      _id: { sensor: "$sensor_id", tipo: "$tipo" },
      mediciones: { $sum: 1 },
      primera: { $min: "$timestamp" },
      ultima: { $max: "$timestamp" }
    }
  },
  {
    $project: {
      _id: 0,
      sensor_id: "$_id.sensor",
      tipo: "$_id.tipo",
      mediciones: 1,
      primera: 1,
      ultima: 1
    }
  },
  { $sort: { mediciones: -1 } }
], { allowDiskUse: true })
