// Parámetros por defecto: sensor=S-011, limite=50
// Uso: mongosh smartcity mongo/queries/MG-Q01.js
// MG-Q01 — Lecturas de un sensor específico
db.lectura.find({ sensor_id: "S-011" }, {
  _id: 0,
  timestamp: 1,
  tipo: 1,
  "ubicacion.zona": 1,
  mediciones: 1,
  "dispositivo.firmware": 1
}).sort({ timestamp: -1 }).limit(50)
