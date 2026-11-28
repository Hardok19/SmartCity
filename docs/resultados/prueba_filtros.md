# Prueba de filtros del dashboard (D3)

Generado por `scripts/prueba_filtros.py` el 2026-11-27 23:39 UTC en 29 s. Filtro base: año 2024 completo, todo el día, todas las zonas.

**Resultado:** 42 combinaciones cambian como se espera, 0 no cambian, 0 sin datos.

| Filtro cambiado | Función (la que usa el dashboard) | Fuente | Estado | Base → filtrado |
|---|---|---|---|---|
| Rango de fechas | `kpis_movilidad` | Spark | ✅ cambia | vehiculos_detectados=20.409.547 → vehiculos_detectados=1.786.126 |
| Rango de fechas | `kpis_energia` | Spark | ✅ cambia | consumo_total_kwh=3.707.776.296 → consumo_total_kwh=340.565.181 |
| Rango de fechas | `lecturas_pg` | PostgreSQL | ✅ cambia | 200.000 filas → 123.610 filas |
| Rango de fechas | `lecturas_trafico_energia` | Spark | ✅ cambia | 87.840 filas → 7.440 filas |
| Rango de fechas | `kpis_ambiente` | MongoDB | ✅ cambia | pm25_promedio=11 → pm25_promedio=12 |
| Rango de fechas | `kpis_iot_mongo` | MongoDB | ✅ cambia | mediciones_recibidas=552.434 → mediciones_recibidas=47.674 |
| Rango de fechas | `serie_temporal` | MongoDB | ✅ cambia | 1.830 filas → 155 filas |
| Rango de fechas | `lecturas_mongo` | MongoDB | ✅ cambia | 5.000 filas → 5.000 filas |
| Perfil horario (hora pico) | `kpis_movilidad` | Spark | ✅ cambia | vehiculos_detectados=20.409.547 → vehiculos_detectados=8.957.030 |
| Perfil horario (hora pico) | `kpis_energia` | Spark | ✅ cambia | consumo_total_kwh=3.707.776.296 → consumo_total_kwh=1.240.962.126 |
| Perfil horario (hora pico) | `lecturas_pg` | PostgreSQL | ✅ cambia | 200.000 filas → 200.000 filas |
| Perfil horario (hora pico) | `lecturas_trafico_energia` | Spark | ✅ cambia | 87.840 filas → 25.620 filas |
| Perfil horario (hora pico) | `kpis_ambiente` | MongoDB | ✅ cambia | pm25_promedio=11 → pm25_promedio=12 |
| Perfil horario (hora pico) | `serie_temporal` | MongoDB | ✅ cambia | 1.830 filas → 1.830 filas |
| Perfil horario (hora pico) | `lecturas_mongo` | MongoDB | ✅ cambia | 5.000 filas → 5.000 filas |
| Zona (Centro) | `kpis_movilidad` | Spark | ✅ cambia | vehiculos_detectados=20.409.547 → vehiculos_detectados=5.943.738 |
| Zona (Centro) | `kpis_energia` | Spark | ✅ cambia | consumo_total_kwh=3.707.776.296 → consumo_total_kwh=629.730.626 |
| Zona (Centro) | `lecturas_pg` | PostgreSQL | ✅ cambia | 200.000 filas → 200.000 filas |
| Zona (Centro) | `lecturas_trafico_energia` | Spark | ✅ cambia | 87.840 filas → 17.568 filas |
| Zona (Centro) | `kpis_iot` | PostgreSQL | ✅ cambia | sensores=50 → sensores=10 |
| Zona (Centro) | `sensores` | PostgreSQL | ✅ cambia | 50 filas → 10 filas |
| Zona (Centro) | `kpis_ambiente` | MongoDB | ✅ cambia | pm25_promedio=11 → pm25_promedio=13 |
| Zona (Centro) | `comparativa_zonas` | MongoDB | ✅ cambia | 5 filas → 1 filas |
| Zona (Centro) | `lecturas_mongo` | MongoDB | ✅ cambia | 5.000 filas → 5.000 filas |
| Tipo de medición (trafico) | `lecturas_pg` | PostgreSQL | ✅ cambia | 200.000 filas → 43.920 filas |
| Tipo de medición (trafico) | `lecturas_trafico_energia` | Spark | ✅ cambia | 87.840 filas → 43.920 filas |
| Tipo de medición (trafico) | `kpis_iot` | PostgreSQL | ✅ cambia | sensores=50 → sensores=5 |
| Tipo de medición (trafico) | `sensores` | PostgreSQL | ✅ cambia | 50 filas → 5 filas |
| Tipo de medición (trafico) | `kpis_iot_mongo` | MongoDB | ✅ cambia | mediciones_recibidas=552.434 → mediciones_recibidas=0 |
| Tipo de medición (trafico) | `distribucion_tipos` | MongoDB | ✅ cambia | 2 filas → 0 filas |
| Tipo de medición (temperatura) | `kpis_iot` | PostgreSQL | ✅ cambia | sensores=50 → sensores=15 |
| Tipo de medición (temperatura) | `sensores` | PostgreSQL | ✅ cambia | 50 filas → 15 filas |
| Tipo de medición (temperatura) | `kpis_iot_mongo` | MongoDB | ✅ cambia | mediciones_recibidas=552.434 → mediciones_recibidas=202.698 |
| Tipo de medición (temperatura) | `distribucion_tipos` | MongoDB | ✅ cambia | 2 filas → 1 filas |
| Tipo de medición (temperatura) | `lecturas_mongo` | MongoDB | ✅ cambia | 5.000 filas → 5.000 filas |
| Tipo de medición (temperatura) | `lecturas_pg` | PostgreSQL | ✅ cambia | 200.000 filas → 122.586 filas |
| Sensor (T-003) | `lecturas_pg` | PostgreSQL | ✅ cambia | 200.000 filas → 8.784 filas |
| Sensor (T-003) | `lecturas_trafico_energia` | Spark | ✅ cambia | 87.840 filas → 8.784 filas |
| Sensor (T-003) | `kpis_movilidad` | Spark | ✅ cambia | vehiculos_detectados=20.409.547 → vehiculos_detectados=5.943.738 |
| Sensor (T-003) | `sensores` | PostgreSQL | ✅ cambia | 50 filas → 1 filas |
| Rango de valores (≥ 1.000) | `lecturas_pg` | PostgreSQL | ✅ cambia | 200.000 filas → 46.473 filas |
| Rango de valores (≥ 1.000) | `lecturas_trafico_energia` | Spark | ✅ cambia | 87.840 filas → 46.273 filas |

Las filas «sin datos» corresponden a fuentes vacías en este entorno (por ejemplo, MongoDB antes de cargar las lecturas); al cargarlas se vuelve a ejecutar el script.