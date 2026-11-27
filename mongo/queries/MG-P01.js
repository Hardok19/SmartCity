// Parámetros por defecto: desde=2024-02-15, hasta=2024-06-15
// Uso: mongosh smartcity mongo/queries/MG-P01.js
// MG-P01 — ¿Cuál es la temperatura promedio por zona?
db.lectura.aggregate([
  {
    $match: {
      tipo: "clima",
      timestamp: {
        $gte: ISODate("2024-02-15T00:00:00.000Z"),
        $lte: ISODate("2024-06-15T23:59:59.999Z")
      }
    }
  },
  {
    $group: {
      _id: "$ubicacion.zona",
      temperaturaPromedio: { $avg: "$mediciones.temperatura" },
      minima: { $min: "$mediciones.temperatura" },
      maxima: { $max: "$mediciones.temperatura" }
    }
  },
  {
    $project: {
      _id: 0,
      zona: "$_id",
      temperaturaPromedio: { $round: ["$temperaturaPromedio", 2] },
      minima: 1,
      maxima: 1
    }
  },
  { $sort: { temperaturaPromedio: -1 } }
], { allowDiskUse: true })
