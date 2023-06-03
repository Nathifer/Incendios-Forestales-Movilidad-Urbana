# Databricks notebook source
# MAGIC %md
# MAGIC # AEMET

# COMMAND ----------

# MAGIC %md
# MAGIC ### Estaciones

# COMMAND ----------

!pip install tqdm 

# COMMAND ----------

import requests
import datetime
import pandas as pd
from tqdm import tqdm

# COMMAND ----------

# Cargamos la api key 
api_key = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJuYXRoYWxpYWZlcm5hbmRlenI5NUBnbWFpbC5jb20iLCJqdGkiOiI0YzVlN2Q4NS1hYTc2LTQ2ZGUtYTM1ZC02NDdjZTQ3Yjc5MGYiLCJpc3MiOiJBRU1FVCIsImlhdCI6MTY2MDA0MTEzNCwidXNlcklkIjoiNGM1ZTdkODUtYWE3Ni00NmRlLWEzNWQtNjQ3Y2U0N2I3OTBmIiwicm9sZSI6IiJ9.pThNL0YZyUC2utBPxEbhKHM16LZmXkVMQWkL9uJGWW0"
querystring = {"api_key": api_key}

# Obtenemos información de todas las estaciones disponibles
url = "https://opendata.aemet.es/opendata/api/valores/climatologicos/inventarioestaciones/todasestaciones"

# Realizamos la request
r = requests.get(url, params=querystring, verify=False)

# Obtenemos el link del que descargaremos los datos
data_url = r.json()['datos']
r_data = requests.get(data_url, params=querystring, verify=False)

#Creamos el DataFrame 
spark.createDataFrame(r_data.json()).coalesce(1).write.mode("overwrite").parquet(f"/mnt/IncendiosForestalesCAT/raw/aemet/estaciones/")

# COMMAND ----------

display(spark.read.parquet(f"/mnt/IncendiosForestalesCAT/raw/aemet/estaciones/"))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC + **Altitud de la estacion:** altitud en la que se encuentra ubicada la estacion 
# MAGIC + **Indicativo:** Codigo de la estacion 
# MAGIC + **Indsinop:** codigo sinoptico usado para reportar observaciones meteorológicas hechas por estaciones meteorológicas en superficie tanto así como por estaciones meteorológicas automáticas.
# MAGIC + **Latitud:** Latitud en la que se encuentra ubicada la estacion
# MAGIC + **Longitud:** Longitud en la que se encuentra ubicada la estacion
# MAGIC + **Nombre:** Nombre de la estacion
# MAGIC + **Provincia:** Provincia en la que se encuentra ubicada la estacion

# COMMAND ----------

# MAGIC %md
# MAGIC ### Valores Climatologicos

# COMMAND ----------

# CARGAR VALORES CLIMATOLOGICOS DIARIOS AEMET
from dateutil.relativedelta import relativedelta

fechaini="2015-01-01"
fechafin="2022-02-01"
start = datetime.datetime.strptime(fechaini, '%Y-%m-%d').date()
end = datetime.datetime.strptime(fechafin, '%Y-%m-%d').date()

dates = pd.date_range(start, end, freq='m')

print(f'Downloading files for the period {start} - {end}')

s = requests.Session()

for d in tqdm(dates):
    vcm = ("https://opendata.aemet.es/opendata/api/valores/climatologicos/diarios/datos"
       f"/fechaini/{start}T00:00:00UTC/fechafin/{d:%Y-%m-%d}T00:00:00UTC/todasestaciones")
    r = requests.get(vcm, params=querystring, verify=False)

    if r.status_code == requests.codes.OK:
        data_url = r.json()['datos']
        r_data = requests.get(data_url, params=querystring, verify=False)
        raw_data = r_data.json()
        spark.createDataFrame(raw_data).coalesce(1).write.mode("append").parquet(f"/mnt/IncendiosForestalesCAT/raw/aemet/valores_clima_diarios/")
    start= start + relativedelta(months=1)

# COMMAND ----------

display(spark.read.parquet(f"/mnt/IncendiosForestalesCAT/raw/aemet/valores_clima_diarios/"))

# COMMAND ----------

# MAGIC %md
# MAGIC + **Altitud:** Altitud de la estación, en metros
# MAGIC + **dir:** Dirección del viento, en decenas de grado
# MAGIC + **fecha:** fecha de medicion del valor
# MAGIC + **horaPresMax:** Hora de la presión máxima, redondeada a la hora entera más próxima
# MAGIC + **horaPresMin:** Hora de la presión mínima, redondeada a la hora entera más próxima
# MAGIC + **horaracha:** Hora de la racha máxima (en formato hh:mm)
# MAGIC + **horatmax:** Hora de la temperatura máxima (en formato hh:mm)
# MAGIC + **horatmin:** Hora de la temperatura mínima (en formato hh:mm)
# MAGIC + **indicativo:** Indicativo climatológico de la estación
# MAGIC + **nombre:** Nombre de la estación
# MAGIC + **prec:** Precipitación diaria, en milímetros
# MAGIC + **presMax:** Presión máxima diaria, en hPa
# MAGIC + **presMin:** Presión mínima diaria, en hPa
# MAGIC + **provincia:** Nombre de la provincia
# MAGIC + **racha:** Racha máxima del viento, en m/sg
# MAGIC + **sol:** Duración de la insolación, en horas
# MAGIC + **tmax:** Temperatura máxima diaria, en grados centígrados
# MAGIC + **tmed:** Temperatura media diaria, en grados centígrados
# MAGIC + **tmin:** Temperatura mínima diaria, en grados centígrados
# MAGIC + **velmedia:** Velocidad media del viento, en m/sg

# COMMAND ----------

# MAGIC %md
# MAGIC ##FWI

# COMMAND ----------

from functools import reduce

def load_table(file_location, 
               table_name, 
               rename_columns=None, 
               file_type="csv", 
               delimiter=";"):
  
  # The applied options are for CSV files. For other file types, these will be ignored.
  df = spark.read.format(file_type) \
    .option("inferSchema", "true") \
    .option("header", "true") \
    .option("sep", delimiter) \
    .load(file_location)
  df.columns
  if rename_columns is not None:
    df = reduce(lambda dfl, x: dfl.withColumnRenamed(x,rename_columns[x]),rename_columns, df)
    df.columns

  # Create a view or table
  df.createOrReplaceTempView(table_name)

  df.write.format("parquet").saveAsTable(table_name)

# COMMAND ----------

# TABLA DEL INDICE FWI POR PROVINCIAS
file_location = "/mnt/IncendiosForestalesCAT/raw/FWI_incendios/"
file_type = "csv"
table_name = "fwi_provincias"

load_table(file_location, table_name)

# COMMAND ----------

fwi_provincias = spark.sql("""
SELECT * FROM fwi_provincias WHERE Provincia in ('Barcelona', 'Tarragona', 'Girona', 'Lleida');
""")
display(fwi_provincias)