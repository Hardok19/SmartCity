# Modelo documental — MongoDB (`smartcity_db`)

Diagrama de las cuatro colecciones principales y de las referencias lógicas entre ellas. Las líneas punteadas son **referencias por valor** (`sensor_id`, `zona.nombre`), no claves foráneas: MongoDB no las hace cumplir. La integridad del catálogo de sensores y zonas la garantiza PostgreSQL.

```mermaid
classDiagram
    direction LR

    class lectura {
        sensor_id : string  «= id_sensor de PG»
        tipo : enum  calidad_aire | clima | trafico | energia | …
        timestamp : Date (UTC)
        origen : openaq | open-meteo | sintetico
        ubicacion : Ubicacion  «embebido»
        mediciones : object  «campos según tipo/firmware»
        dispositivo? : Dispositivo  «solo sensores virtuales»
        calidad_dato? : CalidadDato  «solo si hay anomalía»
    }
    class Ubicacion {
        id_zona : int
        zona : string
        tipo_zona : string
        geo : GeoJSON Point [lon, lat]
    }
    class Mediciones_aire {
        pm25 pm10 co no2 : double
        o3? co2? : double  «firmware 2.x»
    }
    class Mediciones_clima {
        temperatura humedad : double
        precipitacion viento : double
    }
    class Dispositivo {
        firmware : 1.4.2 | 2.1.0
        bateria_pct? : double  «firmware 2.x»
    }
    class CalidadDato {
        valido : bool
        flags : string[]  pico | negativo | valor_pegado | fuera_de_rango
    }

    class evento {
        tipo_evento : contaminacion_alta | falla_sensor | lluvia_intensa | ola_calor | …
        severidad : baja | media | alta | critica
        inicio : Date
        fin? : Date
        zona : ZonaRef  «embebido»
        sensores? : string[]
        detalles : object  «distinto por tipo_evento»
    }
    class alerta {
        regla : string
        variable : string
        valor : double
        umbral : double
        nivel : media | alta | critica
        sensor_id : string
        zona : ZonaRef  «embebido»
        emitida : Date
        estado : abierta | atendida | cerrada
        acciones : Accion[]  «embebido»
    }
    class incidente {
        categoria : enum (8)
        titulo : string  «índice text»
        descripcion : string  «índice text»
        reportado : Date
        zona : ZonaRef  «embebido»
        estado_actual : abierto | en_proceso | resuelto
        prioridad : baja | media | alta
        historial : Cambio[]  «embebido, largo variable»
        reportes_ciudadanos : Reporte[]  «embebido, largo variable»
        heridos? : int  «solo accidente_transito»
        adjuntos? : Adjunto[]
    }
    class ZonaRef {
        id_zona : int
        nombre : string
        geo : GeoJSON Point
    }

    lectura *-- Ubicacion
    lectura *-- Mediciones_aire : tipo = calidad_aire
    lectura *-- Mediciones_clima : tipo = clima
    lectura *-- Dispositivo
    lectura *-- CalidadDato
    evento *-- ZonaRef
    alerta *-- ZonaRef
    incidente *-- ZonaRef
    alerta ..> lectura : sensor_id (derivada de lecturas sobre umbral)
    evento ..> lectura : sensores[] (derivado de lecturas)
```

## Colecciones auxiliares

| Colección | Uso | Validador |
|---|---|---|
| `demo_crud` | Demostración CRUD desde la UI (B2.3); nunca se toca `lectura` | No |
| `lectura_tmp` | Ejemplo de índice TTL (`creado`, expira a la hora) | No |

## Índices

| Colección | Índice | Para qué |
|---|---|---|
| `lectura` | `{sensor_id: 1, timestamp: -1}` | MG-Q01 (últimas lecturas de un sensor) |
| `lectura` | `{tipo: 1, "ubicacion.zona": 1, timestamp: -1}` | MG-Q02/Q06/Q07 y filtros del dashboard |
| `lectura` | `{timestamp: 1}` | Rangos de fechas y exportación |
| `lectura` | `{"ubicacion.geo": "2dsphere"}` | Consultas geoespaciales |
| `evento` | `{tipo_evento: 1, severidad: 1, inicio: -1}` | MG-Q04 |
| `evento`, `alerta` | `{"zona.geo": "2dsphere"}` | `$geoNear` (MG-Q05 con radio) |
| `alerta` | `{"zona.nombre": 1, emitida: -1}` | MG-Q05 |
| `alerta` | `{estado: 1}` parcial (`estado: "abierta"`) | Alertas abiertas; índice pequeño |
| `incidente` | `{categoria: 1, reportado: -1}` | MG-Q08 |
| `incidente` | `text` en `titulo` (peso 3) y `descripcion`, idioma español | Búsqueda de texto libre |
| `lectura_tmp` | `{creado: 1}` TTL 3600 s | Datos temporales que se borran solos |

Definición: [mongo/01_colecciones.js](../../mongo/01_colecciones.js), [mongo/02_indices.js](../../mongo/02_indices.js) y su equivalente en Python, [scripts/mongo_admin.py](../../scripts/mongo_admin.py).
