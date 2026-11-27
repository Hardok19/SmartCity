# Diagrama entidad-relación — PostgreSQL (`smartcity_db`)

Las cinco entidades mínimas del enunciado (en negrita en la tabla de abajo) más las tablas que exige la 3FN y las del flujo de datos. DDL: [sql/ddl/](../../sql/ddl/) (ejecutar con `python scripts/pg_init.py`).

```mermaid
erDiagram
    UBICACIONES ||--o{ SENSORES : "tiene"
    CATEGORIAS_SENSOR ||--o{ TIPOS_SENSOR : "agrupa"
    TIPOS_SENSOR ||--o{ SENSORES : "clasifica"
    ORGANISMOS ||--o{ SENSORES : "es responsable de"
    ORGANISMOS ||--o{ USUARIOS : "emplea"
    SENSORES ||--o{ MEDICION_HORARIA : "registra"
    VARIABLES_MEDICION ||--o{ MEDICION_HORARIA : "mide"
    SENSORES ||--o{ SPARK_ANOMALIA : "presenta"
    UBICACIONES ||--o{ AGG_AMBIENTE_HORA_ZONA : "resume"
    UBICACIONES ||--o{ SPARK_KPI_ZONA : "resume"
    CALENDARIO ||..o{ MEDICION_HORARIA : "fecha (JOIN por fecha local)"

    UBICACIONES {
        serial id_ubicacion PK
        varchar nombre_zona UK "Zona Norte … Zona Oeste"
        varchar distrito
        decimal latitud "centroide"
        decimal longitud "centroide"
    }
    CATEGORIAS_SENSOR {
        serial id_categoria PK
        varchar nombre_categoria UK
    }
    TIPOS_SENSOR {
        varchar tipo_sensor PK "calidad_aire, clima, trafico, energia"
        varchar descripcion
        int id_categoria FK
    }
    ORGANISMOS {
        serial id_organismo PK
        varchar nombre UK
        varchar responsable
    }
    SENSORES {
        varchar id_sensor PK "S-001… T-001… E-001…"
        varchar nombre
        varchar tipo_sensor FK
        date fecha_instalacion "<= hoy"
        varchar estado "activo | inactivo"
        int id_ubicacion FK
        int id_organismo FK
        varchar origen "openaq | open-meteo | sintetico"
        boolean envia_datos
        date fecha_baja "solo inactivos"
        decimal latitud "posición propia"
        decimal longitud
    }
    USUARIOS {
        serial id_usuario PK
        varchar nombre
        varchar correo UK "formato validado"
        int id_organismo FK
    }
    VARIABLES_MEDICION {
        varchar codigo PK "pm25 … energia"
        varchar unidad
    }
    MEDICION_HORARIA {
        varchar id_sensor PK,FK
        timestamptz timestamp PK "partición mensual"
        varchar variable PK,FK
        double valor
        varchar origen
    }
    CALENDARIO {
        date fecha PK
        smallint dia_semana
        boolean es_fin_de_semana
        boolean es_feriado
        varchar nombre_feriado
    }
    AGG_AMBIENTE_HORA_ZONA {
        varchar zona PK,FK
        timestamptz timestamp PK
        double pm25_promedio
        double temperatura_promedio
        double humedad_promedio
        bigint total_mediciones
    }
    SPARK_KPI_ZONA {
        varchar zona PK,FK
        date fecha PK
        varchar metrica PK
        double valor
    }
    SPARK_ANOMALIA {
        varchar id_sensor PK,FK
        timestamptz timestamp PK
        varchar variable PK
        double valor
        varchar tipo_anomalia "marca_calidad, z_score, iqr"
    }
```

| Tabla | Tipo | Origen de los datos |
|---|---|---|
| **ubicaciones** | maestra (entidad mínima) | `config/zonas.yaml` |
| **categorias_sensor** | maestra (entidad mínima) | DDL |
| tipos_sensor | maestra (3FN) | DDL |
| **organismos** | maestra (entidad mínima) | `data/maestros/sensores.csv` + 2 sin sensores |
| **sensores** | maestra (entidad mínima) | `data/maestros/sensores.csv` + contadores T-00x y medidores E-00x |
| **usuarios** | maestra (entidad mínima) | 40 ficticios (`scripts/pg_cargar_maestros.py`) |
| variables_medicion, calendario | referencia | DDL |
| medicion_horaria | hechos (particionada por mes) | Spark B3.4 (`spark/jobs/carga_pg.py`) |
| agg_ambiente_hora_zona, spark_kpi_zona, spark_anomalia | resultados de Spark | Spark B3.4 |
| benchmark_resultado | experimentos | `scripts/pg_optimizacion.py` y C1 |

Objetos derivados: vista `v_sensor_detalle`; vistas materializadas `mv_actividad_sensor` y `mv_kpi_diario_zona`; funciones `fn_sensores_por_organismo`, `fn_sensores_sin_actividad`, `fn_kpi_ambiente` y `fn_crear_particiones`; procedimiento `sp_refrescar_mv`.
