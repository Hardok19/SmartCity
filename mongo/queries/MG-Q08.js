// Parámetros por defecto: desde=2024-02-15, hasta=2024-06-15, texto=
// Uso: mongosh smartcity mongo/queries/MG-Q08.js
// MG-Q08 — Incidentes registrados por categoría
db.incidente.aggregate([
  {
    $match: {
      reportado: {
        $gte: ISODate("2024-02-15T00:00:00.000Z"),
        $lte: ISODate("2024-06-15T23:59:59.999Z")
      }
    }
  },
  {
    $group: {
      _id: "$categoria",
      incidentes: { $sum: 1 },
      resueltos: { $sum: { $cond: [{ $eq: ["$estado_actual", "resuelto"] }, 1, 0] } },
      reportes_ciudadanos: { $sum: { $size: "$reportes_ciudadanos" } },
      heridos: { $sum: { $ifNull: ["$heridos", 0] } },
      con_adjuntos: { $sum: { $cond: [{ $isArray: "$adjuntos" }, 1, 0] } }
    }
  },
  {
    $project: {
      _id: 0,
      categoria: "$_id",
      incidentes: 1,
      resueltos: 1,
      reportes_ciudadanos: 1,
      heridos: 1,
      con_adjuntos: 1,
      pct_resuelto: { $round: [{ $multiply: [{ $divide: ["$resueltos", "$incidentes"] }, 100] }, 1] }
    }
  },
  { $sort: { incidentes: -1 } }
], { allowDiskUse: true })
