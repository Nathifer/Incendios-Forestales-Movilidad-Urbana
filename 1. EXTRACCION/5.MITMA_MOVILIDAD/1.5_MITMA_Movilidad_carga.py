# Databricks notebook source
#Paquetes
from pyspark.sql.functions import *
import pyspark.sql.functions as F
from functools import reduce

# COMMAND ----------

# MAGIC %md
# MAGIC ### Open Data Movilidad
# MAGIC 
# MAGIC Se disponen de los datos de movilidad en España durante el periodo de pandemia COVID-19 a nivel nacional obtenidos utilizando como fuente principal de datos registros anonimizados del posicionamiento de los teléfonos móviles El estudio parte de una muestra de datos de más de 13 millones de líneas móviles proporcionada por un operador móvil, que podría ser incrementada a lo largo del proyecto, en la medida que se cuente con los datos de más operadores.
# MAGIC 
# MAGIC **Datos disponibles en:** https://www.mitma.gob.es/ministerio/covid-19/evolucion-movilidad-big-data/opendata-movilidad
# MAGIC 
# MAGIC Para mayor información, se dispone del informe metodologico en el siguiente enlace: https://cdn.mitma.gob.es/portal-web-drupal/covid-19/bigdata/mitma_-_estudio_movilidad_covid-19_informe_metodologico_v3.pdf

# COMMAND ----------

#Creamos df_movilidad 
df_movilidad = spark.read.format("csv") \
.option("inferSchema", "true") \
.option("header", "true") \
.option("sep", "|") \
.load("/mnt/movilidadforest2020/movilidad/maestra1/municipios")

#Añadimos metadatos
df_movilidad = df_movilidad.withColumn('cod_mun_origen', split(df_movilidad["origen"], '_').getItem(0))\
.withColumn('cod_mun_destino', split(df_movilidad["destino"], '_').getItem(0))\
.withColumn("filename", F.input_file_name())\
.withColumn("month", F.regexp_extract(F.element_at(F.split(F.col("filename"), '/'),-1),'^(\d{4})(\d{2})(\d{2}).*', 2))\
.withColumn("year", F.regexp_extract(F.element_at(F.split(F.col("filename"), '/'),-1),'^(\d{4})(\d{2})(\d{2}).*', 1))


#RAW DATA
df_movilidad.write.mode("overwrite").partitionBy("year","month").parquet(f"/mnt/movilidadforest2020/raw/movilidad/maestra1/municipios/")
