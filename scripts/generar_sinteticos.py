#!/usr/bin/env python3
"""generar_sinteticos.py — F2.4 + B2.2. Semilla fija 2026 → mismos datos en cada corrida.

Lee   config/zonas.yaml, config/umbrales.yaml, data/maestros/sensores.csv
      (opcional) data/raw/openaq/<id_estacion>/<anio>.csv   [timestamp_utc,parameter,value,unit]
      (opcional) data/raw/openmeteo/zona_<norte|sur|centro|este|oeste>_<anio>.json
Escribe data/interim/mongo/{lectura,alerta,evento,incidente}.jsonl (Extended JSON, fechas {"$date": ...})
        data/interim/mongo/resumen.json

Modelo (detalle y fórmulas en docs/datos/GENERACION_SINTETICA.md):
  1. Serie horaria por zona (2023-01-01..2024-12-31 UTC): clima y 5 contaminantes. Si hay datos reales
     (OpenAQ / Open-Meteo) se usan donde existan; los huecos se rellenan con el modelo paramétrico.
  2. Sensores virtuales: serie de la zona interpolada a su cadencia × sesgo propio + ruido gaussiano,
     ~1 % de anomalías (pico, negativo, valor pegado), huecos de 1–6 h y evolución de esquema
     (firmware 2.x desde 2024-03-01 agrega co2 y bateria_pct).
  3. Sensores reales: sus lecturas horarias tal cual (si no hay archivo → modelo paramétrico, origen «sintetico»).
  4. Alertas, eventos e incidentes DERIVADOS de las lecturas/series (coherentes con los datos).

Uso (raíz del repo):  python scripts/generar_sinteticos.py [--desde 2024-01-01 --hasta 2024-01-31 --limite-sensores 10]
"""
import argparse
import csv
import json
import math
import time
from pathlib import Path

import numpy as np
import yaml

RAIZ = Path(__file__).resolve().parents[1]
SEMILLA = 2026
T0 = np.datetime64("2023-01-01T00", "h")
H = 731 * 24                                   # 2023 (365 d) + 2024 (366 d) = 17.544 horas
HORAS = T0 + np.arange(H).astype("timedelta64[h]")
DIAS = HORAS.astype("datetime64[D]")
HORA = (HORAS - DIAS).astype(int)              # 0..23 UTC
DOW = (DIAS.astype(int) + 3) % 7               # 0 = lunes
DOY = (DIAS - HORAS.astype("datetime64[Y]").astype("datetime64[D]")).astype(int) + 1
FW2_DESDE_H = int((np.datetime64("2024-03-01T00", "h") - T0) / np.timedelta64(1, "h"))
FORM = 2 * math.pi
VARS_AIRE = ["pm25", "pm10", "no2", "o3", "co"]
VARS_CLIMA = ["temperatura", "humedad", "precipitacion", "viento"]
OM_MAPA = {"temperature_2m": "temperatura", "relative_humidity_2m": "humedad",
           "precipitation": "precipitacion", "wind_speed_10m": "viento"}
BASE_PM25 = {"Zona Norte": 12.0, "Zona Sur": 10.0, "Zona Centro": 14.0, "Zona Este": 9.5, "Zona Oeste": 8.0}
BASE_NO2 = {"Zona Norte": 38.0, "Zona Sur": 30.0, "Zona Centro": 55.0, "Zona Este": 32.0, "Zona Oeste": 24.0}
OFFSET_T = {"Zona Norte": 0.4, "Zona Sur": 0.0, "Zona Centro": 0.8, "Zona Este": 0.2, "Zona Oeste": -0.3}


# ------------------------------------------------------------------ utilidades
def cargar_yaml(ruta):
    with open(RAIZ / ruta, encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def D(iso):                       # fecha en Extended JSON
    return {"$date": iso}


def iso_h(i):                     # índice de hora → ISO UTC
    return str(T0 + np.timedelta64(int(i), "h")) + ":00:00Z"


def slug(zona):
    return zona.lower().replace("zona ", "")


def ar1(rng, n, phi, sd):
    e = rng.normal(0, sd * math.sqrt(1 - phi ** 2), n)
    x = np.empty(n)
    x[0] = rng.normal(0, sd)
    for i in range(1, n):
        x[i] = phi * x[i - 1] + e[i]
    return x


def idx_hora(texto):
    t = texto.replace(" ", "T")[:13]
    return int((np.datetime64(t, "h") - T0) / np.timedelta64(1, "h"))


def corridas(mascara):
    """(inicio, fin_exclusivo) de cada tramo True consecutivo."""
    m = np.concatenate(([0], mascara.astype(np.int8), [0]))
    d = np.diff(m)
    return list(zip(np.flatnonzero(d == 1), np.flatnonzero(d == -1)))


# ------------------------------------------------------------------ series horarias por zona
def clima_ciudad(rng):
    base = 11.5 + 7.0 * np.sin(FORM * (DOY - 110) / 365.25) + 3.5 * np.sin(FORM * (HORA - 9) / 24)
    temp = base + ar1(rng, H, 0.995, 3.0)
    wet = np.zeros(H, bool)
    u = rng.random(H)
    est = False
    for i in range(H):
        est = u[i] < (0.82 if est else 0.035 * (1 + 0.15 * math.cos(FORM * (DOY[i] - 15) / 365.25)))
        wet[i] = est
    precip = np.where(wet, rng.exponential(0.7, H) * np.where(rng.random(H) < 0.03, 6.0, 1.0), 0.0)
    hum = np.clip(80 - 1.6 * (temp - 11.5) + ar1(rng, H, 0.97, 5.0) + 8 * wet, 25, 100)
    viento = np.exp(math.log(12.0) + ar1(rng, H, 0.97, 0.4)) * (1 + 0.2 * np.cos(FORM * (DOY - 15) / 365.25))
    return temp, hum, precip, viento, wet


def episodios(rng):
    ep = np.ones(H)
    for _ in range(14):
        ini, largo = int(rng.integers(0, H - 80)), int(rng.integers(24, 73))
        ep[ini:ini + largo] *= 1 + (rng.uniform(2.2, 4.5) - 1) * np.sin(np.linspace(0, math.pi, largo)) ** 2
    for ini in (700, 16900):                    # dos episodios severos (invierno)
        ep[ini:ini + 60] *= 1 + (rng.uniform(5.5, 7.5) - 1) * np.sin(np.linspace(0, math.pi, 60)) ** 2
    return np.minimum(ep, 8.0)                  # tope: episodios solapados no superan ×8


def series_zona(zona, ciudad, ep, id_zona):
    """Serie horaria paramétrica de la zona (clima + contaminantes)."""
    temp, hum, precip, viento, wet = ciudad
    rng = np.random.default_rng([SEMILLA, 1, id_zona])
    s = {}
    s["temperatura"] = temp + OFFSET_T[zona] + ar1(rng, H, 0.9, 0.5)
    s["humedad"] = np.clip(hum + ar1(rng, H, 0.9, 2.5), 20, 100)
    s["precipitacion"] = precip * np.exp(rng.normal(0, 0.3, H))
    s["viento"] = viento * np.exp(rng.normal(0, 0.08, H))
    est = 1 + 0.25 * np.cos(FORM * (DOY - 15) / 365.25)
    rush = np.exp(-((HORA - 8) / 1.6) ** 2) + np.exp(-((HORA - 18) / 2.0) ** 2)
    sem = np.where(DOW >= 5, 0.85, 1.0)
    f_viento = np.exp(-0.035 * (s["viento"] - 12))
    f_lluvia = np.where(s["precipitacion"] > 0.1, 0.8, 1.0)
    pm25 = BASE_PM25[zona] * est * (1 + 0.35 * rush) * sem * ep * f_viento * f_lluvia * np.exp(ar1(rng, H, 0.9, 0.15))
    no2 = BASE_NO2[zona] * (1 + 0.15 * np.cos(FORM * (DOY - 15) / 365.25)) * (1 + 0.6 * rush) * np.where(DOW >= 5, 0.8, 1.0) \
        * np.sqrt(ep) * f_viento * np.exp(ar1(rng, H, 0.9, 0.15))
    sol = np.maximum(0, np.sin(FORM * (HORA - 9) / 24))
    s["pm25"] = pm25
    s["pm10"] = 1.7 * pm25 + rng.normal(0, 2.0, H)
    s["no2"] = no2
    s["o3"] = np.maximum(5, 55 * (1 + 0.5 * np.sin(FORM * (DOY - 110) / 365.25)) * (0.5 + 0.7 * sol) - 0.25 * no2 + rng.normal(0, 5, H))
    s["co"] = 150 + 4 * no2 + rng.normal(0, 25, H)
    for k in ("pm25", "pm10", "no2", "o3", "co"):
        s[k] = np.maximum(s[k], 0.2)
    return s


# ------------------------------------------------------------------ datos reales (opcionales)
def leer_openaq(id_est):
    """→ {variable: array[H] con NaN donde no hay dato}"""
    out = {}
    for f in sorted((RAIZ / "data/raw/openaq" / str(id_est)).glob("*.csv")):
        with open(f, encoding="utf-8") as fh:
            for r in csv.DictReader(fh):
                p = r["parameter"].lower().replace(".", "").replace("_", "")
                if p not in VARS_AIRE or r["value"] in ("", None):
                    continue
                i = idx_hora(r["timestamp_utc"])
                if 0 <= i < H:
                    out.setdefault(p, np.full(H, np.nan))[i] = float(r["value"])
    return out


def leer_openmeteo(zona):
    out = {}
    for f in sorted((RAIZ / "data/raw/openmeteo").glob(f"zona_{slug(zona)}_*.json")):
        with open(f, encoding="utf-8") as fh:
            h = json.load(fh)["hourly"]
        idx = np.array([idx_hora(t) for t in h["time"]])
        ok = (idx >= 0) & (idx < H)
        for src, dst in OM_MAPA.items():
            if src in h:
                v = np.array([np.nan if x is None else x for x in h[src]], dtype=float)
                out.setdefault(dst, np.full(H, np.nan))[idx[ok]] = v[ok]
    return out


# ------------------------------------------------------------------ anomalías
def tipo_anomalia(rng, n):
    """Por muestra: 0 normal, 1 pico, 2 negativo/fuera de rango, 3 valor pegado (~1 % en total)."""
    kind = np.zeros(n, np.int8)
    kind[rng.random(n) < 0.006] = 1
    kind[(kind == 0) & (rng.random(n) < 0.002)] = 2
    for s in np.flatnonzero(rng.random(n) < 0.00006):
        kind[s:s + int(rng.integers(12, 49))] = 3
    return kind


def iso_muestra(i, f_min):
    """índice de muestra (cadencia f_min) → ISO UTC."""
    return str(T0.astype("datetime64[m]") + np.timedelta64(int(i) * f_min, "m")) + ":00Z"


# ------------------------------------------------------------------ construcción de documentos
def doc_ubicacion(z, lat, lon):
    return {"id_zona": z["id_zona"], "zona": z["zona"], "tipo_zona": z["tipo_zona"],
            "geo": {"type": "Point", "coordinates": [round(lon, 6), round(lat, 6)]}}


def zona_ref(z):
    return {"id_zona": z["id_zona"], "nombre": z["zona"],
            "geo": {"type": "Point", "coordinates": [z["lon"], z["lat"]]}}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--salida", default=str(RAIZ / "data/interim/mongo"))
    ap.add_argument("--desde", help="YYYY-MM-DD (UTC), por defecto 2023-01-01")
    ap.add_argument("--hasta", help="YYYY-MM-DD (UTC) inclusive, por defecto 2024-12-31")
    ap.add_argument("--limite-sensores", type=int, help="solo los primeros N sensores (pruebas)")
    ap.add_argument("--incidentes", type=int, default=3000)
    a = ap.parse_args()
    t_ini = time.time()

    zonas = {z["zona"]: z for z in cargar_yaml("config/zonas.yaml")["zonas"]}
    um = cargar_yaml("config/umbrales.yaml")
    with open(RAIZ / "data/maestros/sensores.csv", encoding="utf-8") as fh:
        sensores = list(csv.DictReader(fh))
    if a.limite_sensores:
        sensores = sensores[:a.limite_sensores]
    h_ini = max(0, idx_hora(a.desde + "T00")) if a.desde else 0
    h_fin = min(H, idx_hora(a.hasta + "T00") + 24) if a.hasta else H

    # --- series por zona (+ ancla real si existe)
    rng_c = np.random.default_rng([SEMILLA, 0])
    ciudad = clima_ciudad(rng_c)
    ep = episodios(rng_c)
    serie, n_real, clima_real = {}, {}, {}
    for zn, z in zonas.items():
        s = series_zona(zn, ciudad, ep, z["id_zona"])
        clima_real[zn] = leer_openmeteo(zn)
        n_real[zn] = {}
        for v, arr in clima_real[zn].items():
            ok = np.isfinite(arr)
            s[v] = np.where(ok, arr, s[v])
            n_real[zn][v] = int(ok.sum())
        serie[zn] = s
    aire_real = {}
    for s_ in sensores:
        if s_["origen"] == "openaq":
            aire_real[s_["id_sensor"]] = leer_openaq(s_["id_estacion_openaq"])
            for v, arr in aire_real[s_["id_sensor"]].items():
                ok = np.isfinite(arr)
                serie[s_["zona"]][v] = np.where(ok, arr, serie[s_["zona"]][v])

    # --- lecturas (stream a disco) + alertas/eventos derivados
    salida = Path(a.salida)
    salida.mkdir(parents=True, exist_ok=True)
    dump = lambda o: json.dumps(o, separators=(",", ":"), ensure_ascii=False)
    alertas, eventos = [], []
    cuentas = {"lectura": 0, "por_sensor": {}, "anomalias": 0, "huecos": 0}
    umb = um["pm25"]
    rank_aire_virtual = {}
    f_lect = open(salida / "lectura.jsonl", "w", encoding="utf-8", newline="\n")

    for pos, s_ in enumerate(sensores):
        sid, zn = s_["id_sensor"], s_["zona"]
        z = zonas[zn]
        if s_["envia_datos"] == "false":
            continue
        rng = np.random.default_rng([SEMILLA, 2, pos])
        f_min = int(s_["frecuencia_min"])
        k = 60 // f_min
        n = H * k
        x = np.arange(n) / k                                    # horas desde T0
        dia0 = np.datetime64("2023-01-01")
        ini_h = max(0, int((np.datetime64(s_["fecha_instalacion"]) - dia0) / np.timedelta64(1, "D")) * 24)
        fin_h = H if not s_["fecha_baja"] else int((np.datetime64(s_["fecha_baja"]) - dia0) / np.timedelta64(1, "D")) * 24
        mask = (x >= max(ini_h, h_ini)) & (x < min(fin_h, h_fin))
        es_aire = s_["tipo_metrica"] == "calidad_aire"
        vars_ = VARS_AIRE if es_aire else VARS_CLIMA
        origen = s_["origen"]
        if origen == "openaq" and not aire_real.get(sid):
            origen = "sintetico"                                # sin archivo real → provisional
        if origen == "open-meteo" and not clima_real[zn]:
            origen = "sintetico"
        es_real = origen in ("openaq", "open-meteo")
        ubic = doc_ubicacion(z, float(s_["lat"]), float(s_["lon"]))
        fw2, huecos_largos = False, []

        if es_real:
            # lecturas horarias tal cual vienen; solo las horas con algún dato real
            vals = dict(aire_real[sid]) if origen == "openaq" else dict(clima_real[zn])
            tiene = np.zeros(n, bool)
            for arr in vals.values():
                tiene |= np.isfinite(arr)
            mask &= tiene
            ids = np.flatnonzero(mask)
            kind = np.zeros(n, np.int8)
        else:
            # virtual: serie de la zona interpolada + sesgo propio + ruido gaussiano
            vals = {}
            for v in vars_:
                base = np.interp(x, np.arange(H), serie[zn][v])
                if v == "precipitacion":
                    y = np.maximum(0, base * np.exp(rng.normal(0, 0.25, n)))
                elif v == "humedad":
                    y = np.clip(base + rng.normal(0, 2.0, n), 15, 100)
                elif v == "temperatura":
                    y = base + rng.normal(0, 0.4, n) + rng.normal(0, 0.3)
                else:
                    y = np.maximum(0.2, base * rng.normal(1.0, 0.08) * (1 + rng.normal(0, 0.08, n)) + rng.normal(0, 0.3, n))
                vals[v] = y
            for _ in range(int(rng.integers(15, 30))):          # huecos de 1–6 h
                s0, largo = int(rng.integers(0, n)), int(rng.integers(1, 7)) * k
                mask[s0:s0 + largo] = False
                cuentas["huecos"] += 1
                if largo >= 4 * k:
                    huecos_largos.append((s0, largo))
            kind = tipo_anomalia(rng, n)
            y = vals["pm25" if es_aire else "temperatura"]
            pico, neg = kind == 1, kind == 2
            y[pico] = y[pico] * rng.uniform(4, 8, pico.sum()) if es_aire else y[pico] + 14
            if es_aire:
                y[neg] = -np.abs(rng.normal(3, 1.5, neg.sum()))
            else:
                vals["humedad"][neg] = -np.abs(rng.normal(5, 2, neg.sum()))
            for s0, e0 in corridas(kind == 3):                  # sensor pegado
                y[s0:e0] = y[s0]
            ids = np.flatnonzero(mask)
            if es_aire:                                         # evolución de esquema (firmware 2.x)
                r = rank_aire_virtual.get(zn, 0)
                rank_aire_virtual[zn] = r + 1
                fw2 = (r % 2 == 1)
            if fw2:
                no2_i = np.interp(x, np.arange(H), serie[zn]["no2"])
                vals["co2"] = 415 + 0.8 * no2_i + rng.normal(0, 8, n)
                dias = np.maximum(0, x - FW2_DESDE_H) / 24
                bat = 100 - ((dias * 1.2 + float(rng.uniform(0, 70))) % 70)

        # --- emitir lecturas
        ts = [iso_muestra(i, f_min) for i in ids.tolist()]
        cols = {v: (np.round(arr[ids]) if v in ("humedad", "co2") else np.round(arr[ids], 1)).tolist() for v, arr in vals.items()}
        kinds = kind[ids].tolist()
        flag_nom = {1: "pico", 2: "negativo" if es_aire else "fuera_de_rango", 3: "valor_pegado"}
        n_doc = 0
        for j, i in enumerate(ids.tolist()):
            nuevo_fw = fw2 and i / k >= FW2_DESDE_H
            med = {}
            for v, c in cols.items():
                e = c[j]
                if e is None or e != e:                         # NaN
                    continue
                if (not es_real) and es_aire and v in ("o3", "co2") and not nuevo_fw:
                    continue                                    # firmware 1.x no reporta o3 ni co2
                med[v] = e
            if not med:
                continue
            doc = {"sensor_id": sid, "tipo": "calidad_aire" if es_aire else "clima", "timestamp": D(ts[j]),
                   "ubicacion": ubic, "mediciones": med, "origen": origen}
            if not es_real:
                doc["dispositivo"] = {"firmware": "2.1.0", "bateria_pct": round(float(bat[i]), 1)} if nuevo_fw else {"firmware": "1.4.2"}
            if kinds[j]:
                doc["calidad_dato"] = {"valido": False, "flags": [flag_nom[kinds[j]]]}
                cuentas["anomalias"] += 1
            elif es_real and any(e < 0 for e in med.values()):
                doc["calidad_dato"] = {"valido": False, "flags": ["negativo"]}
            f_lect.write(dump(doc) + "\n")
            n_doc += 1
        cuentas["lectura"] += n_doc
        cuentas["por_sensor"][sid] = n_doc

        # --- alertas derivadas: pm25 por nivel / temperatura alta (solo lecturas válidas)
        prim = "pm25" if es_aire else "temperatura"
        if prim in vals and len(ids):
            valida = np.zeros(n, bool)
            valida[ids[kind[ids] == 0]] = True
            serie_p = np.where(valida, vals[prim], np.nan)
            limite = umb["media"] if es_aire else um["temperatura"]["ola_calor"]
            for s0, e0 in corridas(np.isfinite(serie_p) & (serie_p >= limite)):
                t_idx = s0 + int(np.nanargmax(serie_p[s0:e0]))
                pico_v = float(serie_p[t_idx])
                if es_aire:
                    num = 3 if pico_v >= umb["critica"] else 2 if pico_v >= umb["alta"] else 1
                    nivel, umbral = ("media", "alta", "critica")[num - 1], (umb["media"], umb["alta"], umb["critica"])[num - 1]
                    reg, var = f"pm25_nivel_{num}", "pm25"
                else:
                    nivel = "alta" if pico_v >= um["temperatura"]["ola_calor_alta"] else "media"
                    umbral = um["temperatura"]["ola_calor_alta" if nivel == "alta" else "ola_calor"]
                    reg, var = "temperatura_alta", "temperatura"
                emitida = iso_muestra(t_idx, f_min)
                reciente = t_idx / k > H - 24 * 7
                estado = "abierta" if (reciente or rng.random() < 0.05) else ("atendida" if rng.random() < 0.28 else "cerrada")
                acc = [] if estado == "abierta" else [{"fecha": D(emitida), "usuario": "operador@smartcity.example",
                                                       "accion": ("Inspección en sitio", "Notificación a la ciudadanía", "Revisión del sensor")[int(rng.integers(0, 3))]}]
                alertas.append({"regla": reg, "variable": var, "valor": round(pico_v, 1), "umbral": umbral, "nivel": nivel,
                                "sensor_id": sid, "zona": zona_ref(z), "emitida": D(emitida), "estado": estado, "acciones": acc})
                if es_aire:
                    dur = (e0 - s0) / k
                    fin_ev = np.datetime_as_string(np.datetime64(emitida[:-1]) + np.timedelta64(int(max(1.0, dur) * 60), "m"), unit="s") + "Z"
                    eventos.append({"tipo_evento": "contaminacion_alta", "severidad": nivel, "inicio": D(emitida), "fin": D(fin_ev),
                                    "zona": zona_ref(z), "sensores": [sid],
                                    "detalles": {"variable": "pm25", "valor_max": round(pico_v, 1), "umbral": umbral, "duracion_h": round(dur, 1)}})
        # --- eventos de falla de sensor (huecos largos y baja definitiva)
        for s0, largo in huecos_largos:
            if max(ini_h, h_ini) <= s0 / k < min(fin_h, h_fin):
                eventos.append({"tipo_evento": "falla_sensor", "severidad": "baja" if largo / k < 6 else "media",
                                "inicio": D(iso_muestra(s0, f_min)), "zona": zona_ref(z), "sensores": [sid],
                                "detalles": {"motivo": "sin_datos", "horas_sin_datos": largo / k}})
        if s_["fecha_baja"]:
            eventos.append({"tipo_evento": "falla_sensor", "severidad": "media", "inicio": D(s_["fecha_baja"] + "T00:00:00Z"),
                            "zona": zona_ref(z), "sensores": [sid],
                            "detalles": {"motivo": "sin_comunicacion", "estado_final": "inactivo"}})
    f_lect.close()
    aire_real_resumen = {k_: sorted(v) for k_, v in aire_real.items()}

    # --- eventos de zona derivados de las series horarias (clima)
    for zn, z in zonas.items():
        s = serie[zn]
        for s0, e0 in corridas(s["precipitacion"] >= um["precipitacion"]["intensa"]):
            if h_ini <= s0 < h_fin:
                pico = float(s["precipitacion"][s0:e0].max())
                eventos.append({"tipo_evento": "lluvia_intensa", "severidad": "alta" if pico >= um["precipitacion"]["muy_intensa"] else "media",
                                "inicio": D(iso_h(s0)), "fin": D(iso_h(e0)), "zona": zona_ref(z),
                                "detalles": {"precipitacion_max_mm_h": round(pico, 1), "duracion_h": int(e0 - s0)}})
        for s0, e0 in corridas(s["temperatura"] >= um["temperatura"]["ola_calor"]):
            if e0 - s0 >= 3 and h_ini <= s0 < h_fin:
                tmax = float(s["temperatura"][s0:e0].max())
                eventos.append({"tipo_evento": "ola_calor", "severidad": "alta" if tmax >= um["temperatura"]["ola_calor_alta"] else "media",
                                "inicio": D(iso_h(s0)), "fin": D(iso_h(e0)), "zona": zona_ref(z),
                                "detalles": {"temperatura_max": round(tmax, 1), "horas_sobre_umbral": int(e0 - s0)}})

    # --- incidentes (texto libre en español; esquema variable por categoría)
    rng = np.random.default_rng([SEMILLA, 3])
    vias = {"Zona Norte": ["Holloway Road", "Upper Street", "Essex Road"], "Zona Sur": ["Merton High Street", "London Road", "Kingston Road"],
            "Zona Centro": ["Marylebone Road", "Oxford Street", "Edgware Road"], "Zona Este": ["Woolwich Road", "Blackwall Lane", "Falconwood Parade"],
            "Zona Oeste": ["Richmond Hill", "Lower Richmond Road", "Barnes High Street"]}
    plantillas = {
        "accidente_transito": ("Colisión entre vehículos en {via}", "Se reporta una colisión en {via}, con tráfico lento en ambos sentidos y presencia de servicios de emergencia."),
        "bache": ("Bache profundo en {via}", "Vecinos reportan un bache de gran tamaño en {via} que obliga a los vehículos a invadir el carril contiguo."),
        "semaforo_danado": ("Semáforo fuera de servicio en {via}", "El semáforo de la intersección en {via} permanece apagado desde la mañana y genera congestión."),
        "inundacion": ("Acumulación de agua en {via}", "Tras la lluvia, el drenaje de {via} no da abasto y el agua cubre parte de la calzada."),
        "alumbrado": ("Alumbrado público apagado en {via}", "Varias luminarias de {via} no encienden por la noche; se reportan problemas de seguridad."),
        "residuos": ("Acumulación de residuos en {via}", "Bolsas y muebles abandonados en {via} llevan más de tres días sin ser recogidos."),
        "vandalismo": ("Mobiliario urbano dañado en {via}", "Se reporta grafiti y daños en paraderos y bancas de {via}."),
        "quema_humo": ("Humo y olor a quema cerca de {via}", "Residentes de {via} reportan humo denso y olor a quema; se solicita verificación de la calidad del aire."),
    }
    cats = list(plantillas)
    pesos = np.array([0.17, 0.15, 0.08, 0.07, 0.12, 0.15, 0.06, 0.06]); pesos /= pesos.sum()
    zn_list = list(zonas)
    incidentes = []
    for _ in range(a.incidentes):
        cat = cats[int(rng.choice(len(cats), p=pesos))]
        zn = zn_list[int(rng.integers(0, len(zn_list)))]
        via = vias[zn][int(rng.integers(0, 3))]
        h_rep = int(rng.integers(h_ini, h_fin))
        rep = iso_h(h_rep)
        estado = "resuelto" if (H - h_rep) > 24 * 20 and rng.random() < 0.9 else ("en_proceso" if rng.random() < 0.6 else "abierto")
        t, d = plantillas[cat]
        n_rc = int(rng.integers(0, 7))
        inc = {"categoria": cat, "titulo": t.format(via=via), "descripcion": d.format(via=via), "reportado": D(rep),
               "zona": zona_ref(zonas[zn]), "estado_actual": estado, "prioridad": ["baja", "media", "alta"][int(rng.integers(0, 3))],
               "historial": [{"fecha": D(rep), "estado": "abierto", "nota": "Reporte recibido"}]
               + ([{"fecha": D(iso_h(min(H - 1, h_rep + int(rng.integers(2, 72))))), "estado": estado, "nota": "Actualización de la cuadrilla"}] if estado != "abierto" else []),
               "reportes_ciudadanos": [{"fecha": D(iso_h(min(H - 1, h_rep + int(rng.integers(0, 48))))), "canal": ["app", "telefono", "web"][int(rng.integers(0, 3))],
                                        "comentario": "Reporte adicional de vecino"} for _ in range(n_rc)]}
        if cat == "accidente_transito":
            inc["heridos"] = int(rng.choice([0, 0, 0, 1, 1, 2, 3]))
        if rng.random() < 0.2:
            inc["adjuntos"] = [{"tipo": "foto", "nombre": f"adjunto_{int(rng.integers(1000, 9999))}.jpg"}]
        incidentes.append(inc)

    # --- escritura de colecciones pequeñas
    for nombre, lista, clave in (("alerta", alertas, lambda d: d["emitida"]["$date"]), ("evento", eventos, lambda d: d["inicio"]["$date"]),
                                 ("incidente", incidentes, lambda d: d["reportado"]["$date"])):
        lista.sort(key=clave)
        with open(salida / f"{nombre}.jsonl", "w", encoding="utf-8", newline="\n") as f:
            for d in lista:
                f.write(dump(d) + "\n")
    tam = {n: (salida / f"{n}.jsonl").stat().st_size for n in ("lectura", "alerta", "evento", "incidente")}
    resumen = {"semilla": SEMILLA, "periodo": [a.desde or "2023-01-01", a.hasta or "2024-12-31"], "documentos": {
        "lectura": cuentas["lectura"], "alerta": len(alertas), "evento": len(eventos), "incidente": len(incidentes)},
        "bytes": tam, "anomalias_inyectadas": cuentas["anomalias"], "huecos_inyectados": cuentas["huecos"],
        "sensores_con_datos": len(cuentas["por_sensor"]), "lecturas_por_sensor": cuentas["por_sensor"],
        "datos_reales_openmeteo_horas": n_real, "estaciones_openaq_con_datos": aire_real_resumen,
        "segundos": round(time.time() - t_ini, 1)}
    with open(salida / "resumen.json", "w", encoding="utf-8") as fh:
        json.dump(resumen, fh, indent=1, ensure_ascii=False)
    print(json.dumps({k: resumen[k] for k in ("documentos", "bytes", "anomalias_inyectadas", "huecos_inyectados", "sensores_con_datos", "segundos")}, indent=1))


if __name__ == "__main__":
    main()
