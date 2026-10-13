// Se ejecuta automáticamente la primera vez que se crea el contenedor de Mongo.
// mongosh se autentica aquí como root porque corre dentro del propio contenedor.

db = db.getSiblingDB('smartcity_db');

db.createCollection('mediciones');
db.createCollection('eventos');
db.createCollection('alertas');

// Índices mínimos recomendados para las consultas obligatorias
db.mediciones.createIndex({ sensor_id: 1 });
db.mediciones.createIndex({ timestamp: 1 });
db.mediciones.createIndex({ "ubicacion.zona": 1 });
db.mediciones.createIndex({ tipo: 1 });

db.eventos.createIndex({ timestamp: 1 });
db.eventos.createIndex({ zona: 1 });

print("Colecciones e índices de smartcity_db creados correctamente.");
