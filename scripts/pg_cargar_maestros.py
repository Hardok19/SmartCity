"""Carga config/zonas.yaml y data/maestros/sensores.csv (o data/sensores.csv).
Requiere: pip install psycopg2-binary pyyaml
Ejecutar desde raíz: python scripts/pg_cargar_maestros.py
"""
import csv
import os
from pathlib import Path
import psycopg2
import yaml

ROOT=Path(__file__).resolve().parents[1]
z=yaml.safe_load((ROOT/'config/zonas.yaml').read_text(encoding='utf-8'))['zonas']
p=ROOT/'data/maestros/sensores.csv'
if not p.exists(): p=ROOT/'data/sensores.csv'
if not p.exists(): raise FileNotFoundError('Falta data/maestros/sensores.csv o data/sensores.csv; integrar catálogo de pareja B')
rows=list(csv.DictReader(p.open(encoding='utf-8-sig',newline='')))
conn=psycopg2.connect(host=os.getenv('PGHOST','localhost'),port=int(os.getenv('PGPORT','5432')),
 dbname=os.getenv('PGDATABASE','smartcity_db'),user=os.getenv('PGUSER','smartcity'),password=os.getenv('PGPASSWORD','smartcity123'))
with conn:
 with conn.cursor() as cur:
  for a in z:
   cur.execute('''INSERT INTO ubicaciones(nombre_zona,distrito,latitud,longitud) VALUES (%s,%s,%s,%s)
    ON CONFLICT(nombre_zona) DO UPDATE SET distrito=EXCLUDED.distrito,latitud=EXCLUDED.latitud,longitud=EXCLUDED.longitud''',
    (a['zona'],a['distrito'],a['lat'],a['lon']))
  cats=sorted(set(r['categoria'] for r in rows))
  orgs=sorted(set(r['organismo'] for r in rows))
  for c in cats:
   cur.execute('INSERT INTO categorias_sensor(nombre_categoria) VALUES(%s) ON CONFLICT(nombre_categoria) DO NOTHING',(c,))
  for o in orgs:
   cur.execute('SELECT id_organismo FROM organismos WHERE nombre=%s',(o,))
   if not cur.fetchone(): cur.execute('INSERT INTO organismos(nombre,responsable) VALUES(%s,%s)',(o,'Por asignar'))
  for r in rows:
   cur.execute('''INSERT INTO sensores(id_sensor,nombre,tipo_sensor,fecha_instalacion,estado,id_ubicacion,id_categoria,id_organismo,origen,envia_datos,fecha_baja,id_estacion_openaq,frecuencia_min)
   VALUES (%s,%s,%s,%s,%s,(SELECT id_ubicacion FROM ubicaciones WHERE nombre_zona=%s),
    (SELECT id_categoria FROM categorias_sensor WHERE nombre_categoria=%s),
    (SELECT id_organismo FROM organismos WHERE nombre=%s LIMIT 1),%s,%s,%s,%s,%s)
   ON CONFLICT(id_sensor) DO UPDATE SET nombre=EXCLUDED.nombre,tipo_sensor=EXCLUDED.tipo_sensor,
   fecha_instalacion=EXCLUDED.fecha_instalacion,estado=EXCLUDED.estado,id_ubicacion=EXCLUDED.id_ubicacion,
   id_categoria=EXCLUDED.id_categoria,id_organismo=EXCLUDED.id_organismo,origen=EXCLUDED.origen,
   envia_datos=EXCLUDED.envia_datos,fecha_baja=EXCLUDED.fecha_baja,
   id_estacion_openaq=EXCLUDED.id_estacion_openaq,frecuencia_min=EXCLUDED.frecuencia_min''',
   (r['id_sensor'],r['nombre'],r['tipo_metrica'],r['fecha_instalacion'] or None,r['estado'],r['zona'],r['categoria'],r['organismo'],r['origen'],r['envia_datos'].lower()=='true',r['fecha_baja'] or None,r['id_estacion_openaq'] or None,int(r['frecuencia_min'] or 60)))
  cur.execute('SELECT COUNT(*) FROM sensores')
  print('Sensores cargados:',cur.fetchone()[0])
conn.close()
