"""SOLO PARA PROBAR SQL SIN SPARK: 120.000 lecturas de prueba deterministas (origen = 'prueba_pg').

Los datos reales de medicion_horaria los carga Spark (spark/jobs/carga_pg.py, B3.4), que BORRA
automáticamente las filas 'prueba_pg' antes de cargar. No usar estas lecturas en resultados ni en la defensa.
Requiere psycopg2-binary. Ejecutar tras pg_cargar_maestros.py.
"""
import os
import random
from datetime import datetime,timedelta,timezone
import psycopg2
from psycopg2.extras import execute_values
rng=random.Random(2026)
conn=psycopg2.connect(host=os.getenv('PGHOST','localhost'),port=int(os.getenv('PGPORT','5433')),
 dbname=os.getenv('PGDATABASE','smartcity_db'),user=os.getenv('PGUSER','smartcity'),password=os.getenv('PGPASSWORD','smartcity123'))
with conn:
 with conn.cursor() as cur:
  cur.execute("SELECT id_sensor FROM sensores WHERE envia_datos=true AND tipo_sensor IN ('calidad_aire','clima') ORDER BY id_sensor")
  sensors=[x[0] for x in cur.fetchall()]
  if not sensors: raise RuntimeError('Primero cargue los sensores con pg_cargar_maestros.py')
  start=datetime(2023,1,1,tzinfo=timezone.utc)
  batch=[]
  for i in range(120000):
   sensor=sensors[i%len(sensors)]; moment=start+timedelta(hours=i//len(sensors))
   variable=['pm25','temperatura','humedad'][i%3]
   base={'pm25':15,'temperatura':13,'humedad':75}[variable]
   value=round(base+rng.gauss(0,4 if variable!='humedad' else 8),2)
   if variable=='humedad': value=max(0,min(100,value))
   batch.append((sensor,moment,variable,value,'prueba_pg'))
   if len(batch)==5000:
    execute_values(cur,'''INSERT INTO medicion_horaria(id_sensor,timestamp,variable,valor,origen) VALUES %s
      ON CONFLICT(id_sensor,timestamp,variable) DO NOTHING''',batch,page_size=1000)
    batch.clear()
  if batch: execute_values(cur,'''INSERT INTO medicion_horaria(id_sensor,timestamp,variable,valor,origen) VALUES %s
      ON CONFLICT(id_sensor,timestamp,variable) DO NOTHING''',batch,page_size=1000)
  cur.execute('SELECT COUNT(*) FROM medicion_horaria')
  print('Total lecturas:',cur.fetchone()[0])
conn.close()
