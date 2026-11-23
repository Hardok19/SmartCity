// Parámetros por defecto: variable=pm25, zona=Zona Norte, unidad=hour, desde=2024-04-01, hasta=2024-04-07
// Uso: mongosh smartcity mongo/queries/MG-P05.js
// MG-P05 — ¿Cuál es la evolución de una variable durante un período?
db.lectura.aggregate([
  {
    $match: {
      tipo: "calidad_aire",
      "ubicacion.zona": "Zona Norte",
      timestamp: {
        $gte: ISODate("2024-04-01T00:00:00.000Z"),
        $lte: ISODate("2024-04-07T23:59:59.999Z")
      }
    }
  },
  {
    $group: {
      _id: { $dateTrunc: { date: "$timestamp", unit: "hour", timezone: "UTC" } },
      promedio: { $avg: "$mediciones.pm25" },
      maximo: { $max: "$mediciones.pm25" },
      lecturas: { $sum: 1 }
    }
  },
  {
    $project: {
      _id: 0,
      periodo: "$_id",
      promedio: { $round: ["$promedio", 2] },
      maximo: 1,
      lecturas: 1
    }
  },
  { $sort: { periodo: 1 } }
], { allowDiskUse: true })
