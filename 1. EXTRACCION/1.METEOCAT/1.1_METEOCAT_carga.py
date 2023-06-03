# Databricks notebook source
# MAGIC %md
# MAGIC # METEOCAT

# COMMAND ----------

!pip install tqdm sodapy

# COMMAND ----------

#Paquetes
import pandas as pd
import datetime
import pyspark.sql.functions as F
from tqdm import tqdm

#Estaciones AEMET
import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning

# Datos de METEOCAT
from sodapy import Socrata

# COMMAND ----------

# MAGIC %md
# MAGIC ### Carga de Estaciones de METEOCAT

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Tabla con los metadatos asociados a cada una de las estaciones de la Red de Estaciones Meteorológicas Automáticas (XEMA), integrada en la Red de Equipamientos Meteorológicos de la Generalidad de Cataluña (Xemec), del Servicio Meteorológico de Cataluña. Cada estación se identifica con un código. 
# MAGIC 
# MAGIC **Metadatos disponibles en:**  https://analisi.transparenciacatalunya.cat/Medi-Ambient/Metadades-estacions-meteorol-giques-autom-tiques/yqwd-vj5e

# COMMAND ----------

# Unauthenticated client only works with public data sets. Note 'None' in place of application token, and no username or password:
client = Socrata("analisi.transparenciacatalunya.cat", None)

# First 100000 results, returned as JSON from API / converted to Python list of dictionaries by sodapy.
meteocat_stations = client.get("yqwd-vj5e", limit=100000)

# Convert to pandas DataFrame
meteocat_stations_df = pd.DataFrame.from_records(meteocat_stations)

spark.createDataFrame(meteocat_stations_df.astype(str)).coalesce(1).write.mode("overwrite").parquet(f"/mnt/IncendiosForestalesCAT/raw/meteocat/estaciones/")

# COMMAND ----------

meteocat_stations_df

# COMMAND ----------

# MAGIC %md
# MAGIC ### Carga de Valores Climatologicos de METEOCAT

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Datos meteorológicos registrados en todas las estaciones de la Red de Estaciones Meteorológicas Automáticas (XEMA) del Servicio Meteorológico de Cataluña (METEOCAT). Esta fecha base contiene variables medidas con una frecuencia inferior a la diaria, generalmente semi-horaria.
# MAGIC 
# MAGIC **Metadatos disponibles en:** https://analisi.transparenciacatalunya.cat/Medi-Ambient/Dades-meteorol-giques-de-la-XEMA/nzvn-apee

# COMMAND ----------

from dateutil.relativedelta import relativedelta

fechaini="2015-01-01"
fechafin="2022-02-01"
start = datetime.datetime.strptime(fechaini, '%Y-%m-%d').date()
end = datetime.datetime.strptime(fechafin, '%Y-%m-%d').date()
dates = pd.date_range(start, end, freq='d')

print(f'Downloading files for the period {start} - {end}')

client = Socrata("analisi.transparenciacatalunya.cat", None)
data = []

for d in tqdm(dates):
  print(f'Downloading files for the period {d:%Y-%m-%d}T00:00:00.000 - {d:%Y-%m-%d}T23:59:59.000')
  meteocat_diarios_vcm = client.get("nzvn-apee", where=f"data_lectura >= '{d:%Y-%m-%d}T00:00:00.000' AND data_lectura < '{d:%Y-%m-%d}T23:59:59.000' ", limit=100000000)
  meteocat_diarios_vcmPD = pd.DataFrame.from_records(meteocat_diarios_vcm)
  # Debido que la estacion reporta cada 30 min se filtran los valores de las YYYY-mm-ddT12:00:00.000
  try:
    meteocat_diarios_vcmPD = meteocat_diarios_vcmPD[meteocat_diarios_vcmPD['data_lectura'].str.contains('^\d{4}-\d{2}-\d{2}T12:00:00.000')]
  except KeyError:
    print("data_lectura not found")
    pass
  
  data.append(meteocat_diarios_vcmPD)

# COMMAND ----------

meteocat_diarios_vcmDF = spark.createDataFrame(pd.concat(data))
meteocat_diarios_vcmDF.coalesce(10).write.mode("overwrite").parquet(f"/mnt/IncendiosForestalesCAT/raw/meteocat/valores_clima_diarios/")

# COMMAND ----------

display(spark.read.parquet(f"/mnt/IncendiosForestalesCAT/raw/meteocat/valores_clima_diarios/"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Codigos de Variables de METEOCAT

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Tabla con los metadatos asociados a cada una de las variables de la Red de Estaciones Meteorológicas Automáticas (XEMA), integrada en la Red de Equipamientos Meteorológicos de la Generalidad de Cataluña (Xemec), del Servicio Meteorológico de Cataluña. Cada variable se identifica con un código.
# MAGIC 
# MAGIC **Metadatos disponibles en:** https://analisi.transparenciacatalunya.cat/Medi-Ambient/Metadades-variables-meteorol-giques/4fb2-n3yi

# COMMAND ----------

# Unauthenticated client only works with public data sets. Note 'None' in place of application token, and no username or password:
client = Socrata("analisi.transparenciacatalunya.cat", None)

# Returned as JSON from API / converted to Python list of dictionaries by sodapy.
meteocat_var = client.get("4fb2-n3yi")

# Convert to pandas DataFrame
meteocat_var_df = pd.DataFrame.from_records(meteocat_var)

#Ver datos
meteocat_var_df

spark.createDataFrame(meteocat_var_df.astype(str)).coalesce(1).write.mode("overwrite").parquet(f"/mnt/IncendiosForestalesCAT/raw/meteocat/variables/")

# COMMAND ----------

meteocat_var_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Incendios Forestales en Catalunya

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Relación de los incendios forestales ocurridos en Cataluña desde el 1 de enero de 2011 al 31 de diciembre de 2021, con detalle de la fecha, el municipio en el que se ha iniciado y la superficie forestal y no forestal afectada, en hectáreas.
# MAGIC 
# MAGIC **Metadatos disponibles en:** https://analisi.transparenciacatalunya.cat/Medi-Rural-Pesca/Incendis-forestals-a-Catalunya-Anys-2011-2021/bks7-dkfd

# COMMAND ----------

#INCENDIOS FORESTALES POR COMARCAS EN CATALUÑA 2011 - 2021 -  FILTRAR AÑOS 2020 / 2022
client = Socrata("analisi.transparenciacatalunya.cat", None)
incendios_cat = client.get("bks7-dkfd", where="data_incendi >= '2015-01-01T00:00:00.000' AND data_incendi < '2021-12-31T23:59:59.000'",limit=10000000)

# Convert to pandas DataFrame
incendios_cat_df = pd.DataFrame.from_records(incendios_cat)
spark.createDataFrame(incendios_cat_df.astype(str)).coalesce(1).write.mode("overwrite").parquet(f"/mnt/IncendiosForestalesCAT/raw/meteocat/incendios_forestales/")

# COMMAND ----------

incendios_cat_df.display()