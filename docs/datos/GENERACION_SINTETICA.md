# Generación de datos sintéticos (F2.4 / D-06)

El enunciado exige documentar el mecanismo con el que se amplían los datasets. Script: `scripts/generar_sinteticos.py`
(semilla **2026**; cada sensor usa su propio generador `default_rng([2026, 2, posición])`, así que el resultado es
reproducible y no cambia al usar `--limite-sensores`).

## 1. Qué es real y qué es sintético

| Dato | Origen | Cómo entra |
|---|---|---|
| Contaminantes de las 5 estaciones (PM2.5, PM10, NO₂, O₃, CO) | **OpenAQ** (real) | `descargar_openaq.py` → ancla de la serie de zona y lecturas de los sensores `S-001…S-005` |
| Clima horario por zona (temperatura, humedad, precipitación, viento) | **Open-Meteo** (real) | `descargar_openmeteo.py` → ancla de la serie de zona y sensores `S-006…S-010` |
| Sensores virtuales (30), alertas, eventos, incidentes | **Sintético** | Derivados de las series de zona (abajo) |

Si falta un archivo real, la hora/variable se rellena con el modelo paramétrico y el sensor correspondiente queda con
`origen: "sintetico"` (así se distingue en Mongo y en Spark). Con datos reales presentes, `origen` pasa a `openaq` / `open-meteo`.

## 2. Modelo paramétrico de la serie horaria por zona (2023-01-01 … 2024-12-31 UTC, 17.544 h)

Con `doy` = día del año, `h` = hora UTC, `AR(φ, σ)` = ruido autorregresivo.

- **Temperatura (°C)** = 11,5 + 7,0·sen(2π(doy−110)/365,25) + 3,5·sen(2π(h−9)/24) + AR(0,995; 3,0) [compartido por la ciudad] + offset de zona (Centro +0,8; Norte +0,4; Este +0,2; Sur 0; Oeste −0,3) + AR(0,9; 0,5).
- **Precipitación (mm/h)**: cadena de Markov de dos estados (P(seco→lluvia)=0,035 con leve sesgo invernal; P(lluvia→lluvia)=0,82 ≈ 16 % de horas húmedas); cantidad ~ Exp(0,7) con 3 % de eventos ×6; por zona × lognormal(0; 0,3).
- **Humedad (%)** = 80 − 1,6·(T−11,5) + AR(0,97; 5) + 8·[lluvia], acotada a 25–100.
- **Viento (km/h)** = exp(ln 12 + AR(0,97; 0,4)) · (1 + 0,2·cos estacional).
- **PM2.5 (µg/m³)** = base_zona · estacional · (1 + 0,35·rush) · fin_de_semana · episodio · f_viento · f_lluvia · exp(AR(0,9; 0,15))
  - base_zona: Centro 14, Norte 12, Sur 10, Este 9,5, Oeste 8; estacional = 1 + 0,25·cos(2π(doy−15)/365,25); rush = exp(−((h−8)/1,6)²) + exp(−((h−18)/2)²); fin de semana ×0,85; f_viento = exp(−0,035(viento−12)); f_lluvia = 0,8 si llueve.
  - **Episodios** de contaminación (comunes a toda la ciudad): 14 de 24–72 h con amplitud ×2,2–4,5 y 2 severos (ene-2023, dic-2024) de ×5,5–7,5; el factor total se limita a ×8.
- **PM10** = 1,7·PM2.5 + N(0; 2). **NO₂** = base (Centro 55 … Oeste 24) · estacional · (1 + 0,6·rush) · fin de semana ×0,8 · √episodio · f_viento · lognormal. **O₃** = 55·(1+0,5·sen estacional)·(0,5+0,7·sol) − 0,25·NO₂ + N(0; 5). **CO** (µg/m³) = 150 + 4·NO₂ + N(0; 25).

## 3. Sensores virtuales (30) — 4 de aire y 2 de clima por zona

- Cadencia: 30 min (parámetro `frecuencia_min` de `sensores.csv`); serie de la zona interpolada linealmente.
- Valor = serie · sesgo propio N(1; 0,08) · (1 + N(0; 0,08)) + N(0; 0,3) (clima: ruido aditivo propio).
- **Anomalías (~1 % de las lecturas)**, marcadas en `calidad_dato {valido:false, flags}`: picos ×4–8 (0,6 %), valores negativos / fuera de rango (0,2 %), sensor pegado (tramos de 12–48 lecturas repetidas).
- **Huecos** de 1–6 h (15–30 por sensor). Los de ≥ 4 h generan un evento `falla_sensor`.
- **Evolución de esquema**: la mitad de los virtuales de aire pasa a *firmware 2.x* el 2024-03-01 y desde entonces sus documentos agregan `mediciones.o3`, `mediciones.co2` y `dispositivo.bateria_pct` (los demás nunca los tienen).
- 2 sensores **nunca envían** datos (`envia_datos=false`) y 2 **dejan de enviar** (`estado=inactivo`, `fecha_baja`).
- 6 de los 30 se instalan dentro del período; solo hay datos desde su `fecha_instalacion`.

## 4. Derivados coherentes con las lecturas

- **Alertas**: cada tramo continuo con PM2.5 ≥ 35,4 (o temperatura ≥ 28 °C) en lecturas válidas → una alerta con el valor máximo del tramo; nivel `media/alta/critica` según `config/umbrales.yaml`; estado `abierta` (últimos 7 días + 5 %), `atendida` (~28 %) o `cerrada`.
- **Eventos**: `contaminacion_alta` (de las alertas de PM2.5), `falla_sensor`, `lluvia_intensa` (≥ 4 mm/h) y `ola_calor` (≥ 3 h seguidas ≥ 28 °C) calculados sobre las series de zona.
- **Incidentes** (3.000): categorías y plantillas de texto en español con las vías de cada zona; `reportes_ciudadanos` e `historial` son arreglos de largo variable; `heridos` solo en accidentes; `adjuntos` en ~20 %.

## 5. Volúmenes obtenidos (corrida completa, sin datos reales descargados)

| Colección | Documentos |
|---|---|
| `lectura` | 1.095.798 (≈ 378 MB en JSONL) |
| `alerta` | 884 |
| `evento` | 1.553 |
| `incidente` | 3.000 |

Cumple el mínimo de MongoDB (≥ 500.000) y el de Spark (≥ 1.000.000 procesados). Al incorporar OpenAQ y Open-Meteo reales
los sensores `S-001…S-010` pasan de ~17.500 a la cantidad de horas con dato real.
