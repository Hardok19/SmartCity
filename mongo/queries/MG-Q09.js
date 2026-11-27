// Parámetros por defecto: variable=pm25, zona=(todas), desde=2024-02-15, hasta=2024-06-15
// Uso: mongosh smartcity mongo/queries/MG-Q09.js
// MG-Q09 — Agregación avanzada: resumen, serie con media móvil, histograma y top sensores con sus alertas ($lookup)
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
    $facet: {
      serie_diaria: [
        {
          $group: {
            _id: { $dateTrunc: { date: "$timestamp", unit: "day", timezone: "UTC" } },
            valor: { $avg: "$mediciones.pm25" }
          }
        },
        {
          $setWindowFields: {
            sortBy: { _id: 1 },
            output: { media_movil_7d: { $avg: "$valor", window: { documents: [-6, 0] } } }
          }
        },
        {
          $project: {
            _id: 0,
            dia: "$_id",
            valor: { $round: ["$valor", 2] },
            media_movil_7d: { $round: ["$media_movil_7d", 2] }
          }
        }
      ],
      histograma: [
        {
          $bucket: {
            groupBy: "$mediciones.pm25",
            boundaries: [-1000, 0, 12, 35.4, 55.4, 150.4, 100000],
            default: "sin_dato",
            output: { lecturas: { $sum: 1 } }
          }
        },
        { $project: { _id: 0, desde: "$_id", lecturas: 1 } }
      ],
      top_sensores: [
        {
          $group: {
            _id: "$sensor_id",
            valor: { $avg: "$mediciones.pm25" },
            zona: { $first: "$ubicacion.zona" }
          }
        },
        { $sort: { valor: -1 } },
        { $limit: 5 },
        { $lookup: { from: "alerta", localField: "_id", foreignField: "sensor_id", as: "alertas" } },
        {
          $project: {
            _id: 0,
            sensor_id: "$_id",
            zona: 1,
            valor: { $round: ["$valor", 2] },
            alertas: { $size: "$alertas" }
          }
        }
      ],
      resumen: [
        {
          $group: {
            _id: null,
            lecturas: { $sum: 1 },
            promedio: { $avg: "$mediciones.pm25" },
            maximo: { $max: "$mediciones.pm25" },
            sensores: { $addToSet: "$sensor_id" }
          }
        },
        {
          $project: {
            _id: 0,
            lecturas: 1,
            promedio: { $round: ["$promedio", 2] },
            maximo: 1,
            sensores: { $size: "$sensores" }
          }
        }
      ]
    }
  },
  {
    $project: {
      filas: {
        $concatArrays: [
          { $map: { input: "$resumen", in: { $mergeObjects: ["$$this", { faceta: "resumen" }] } } },
          {
            $map: { input: "$serie_diaria", in: { $mergeObjects: ["$$this", { faceta: "serie_diaria" }] } }
          },
          {
            $map: { input: "$histograma", in: { $mergeObjects: ["$$this", { faceta: "histograma" }] } }
          },
          {
            $map: { input: "$top_sensores", in: { $mergeObjects: ["$$this", { faceta: "top_sensores" }] } }
          }
        ]
      }
    }
  },
  { $unwind: "$filas" },
  { $replaceRoot: { newRoot: "$filas" } }
], { allowDiskUse: true })
