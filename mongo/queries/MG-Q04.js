// Parámetros por defecto: severidad_min=alta, tipo_evento=(todas), zona=(todas), limite=200
// Uso: mongosh smartcity mongo/queries/MG-Q04.js
// MG-Q04 — Eventos críticos detectados
db.evento.find({ severidad: { $in: ["alta", "critica"] } }, {
  _id: 0,
  tipo_evento: 1,
  severidad: 1,
  inicio: 1,
  fin: 1,
  "zona.nombre": 1,
  sensores: 1,
  detalles: 1
}).sort({ inicio: -1 }).limit(200)
