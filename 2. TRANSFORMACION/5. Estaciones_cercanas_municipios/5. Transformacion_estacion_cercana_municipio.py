# Databricks notebook source
# MAGIC %md
# MAGIC #### Actualmente hay 741 municipios donde hubo incendio y solo tenemos 233 estaciones metorológicas. Por lo que si hacemos join de incencios con valores meteorológicos perderemos muchos datos sobre incendios. 
# MAGIC #### El objetivo es tratar de asignar la estacion meteorológica XEMA más cercana  a cada municicipio.

# COMMAND ----------

!pip install geopy tqdm

# COMMAND ----------

from functools import reduce
import pyspark.sql.functions as F
from geopy.geocoders import Nominatim
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
from pyspark.sql.types import StringType
import pandas as pd

# COMMAND ----------

#parquet
incendios_ocurridos_CATDF = spark.read.format("parquet") \
.load("/mnt/IncendiosForestalesCAT/raw/meteocat/incendios_forestales") \
.select(["codi_municipi","termemunic"]) \
.withColumnRenamed("termemunic","municipio") \
.withColumnRenamed("codi_municipi","codigo_municipio")

#parquet
alt_pend_riscDF = spark.read.format("parquet") \
.load("/mnt/IncendiosForestalesCAT/prep/alt_pend_risc") \
.select(["cod_idescat","municipio"]) \
.withColumnRenamed("cod_idescat","codigo_municipio")


# COMMAND ----------

incendios_ocurridos_CATDF.select(["municipio"]).distinct().count()

# COMMAND ----------

alt_pend_riscDF.select(["municipio"]).distinct().count()

# COMMAND ----------

# Obtener municipios que no tienen datos meteo
muni_incendios= incendios_ocurridos_CATDF.distinct()
muni_risc =alt_pend_riscDF.distinct()
missing_muni = muni_risc.alias("df1").join(muni_incendios.alias("df2"), F.col("df1.codigo_municipio")==F.col("df2.codigo_municipio"),"left")
missing_muni.filter(F.col("df2.municipio").isNull()).count()

# COMMAND ----------

missing_muni=missing_muni.select("df1.*")
display(missing_muni)

# COMMAND ----------

# MAGIC %md
# MAGIC Solo tenemos 1 municipio sin datos de cartografia, riesgo incendio y 206 municipios que no tenemos en la tabla incendio. 
# MAGIC Por lo tanto se analizará calculará la distancia entre municipios de alt_pend_riscDF y las estaciones AEMET y Meteocat

# COMMAND ----------

estaciones_meteocatDF = spark.read.parquet("/mnt/IncendiosForestalesCAT/raw/meteocat/estaciones/")\
.filter(F.col("data_fi") >= "nan")\
.select(["codi_estacio", "latitud", "longitud", "nom_municipi"])\
.withColumnRenamed("codi_estacio","indicativo")\
.withColumnRenamed("nom_municipi","nombre")
display(estaciones_meteocatDF)

# COMMAND ----------

@udf(returnType=StringType())
def convertToDec(coord_str):
    """
    Convertir coordenada con formato grados, minutos, segundos a decimales
    """
    direction = coord_str[-1]
    coord = (int(coord_str[:2]) +int(coord_str[2:4])/60 +float(coord_str[4:6])/3600)* (-1 if direction in ['W', 'S'] else 1)
    return (f"{coord}")

# COMMAND ----------

estaciones_aemetDF = spark.read.parquet("/mnt/IncendiosForestalesCAT/raw/aemet/estaciones/")\
.filter(F.col("provincia").isin(["BARCELONA", "TARRAGONA", "GIRONA", "LLEIDA"]))\
.select(["indicativo", "latitud", "longitud", "nombre"]) \
.withColumn("latitud", convertToDec(F.col("latitud"))) \
.withColumn("longitud", convertToDec(F.col("longitud")))
display(estaciones_aemetDF)

# COMMAND ----------

estacionesDF= estaciones_meteocatDF.union(estaciones_aemetDF)
display(estacionesDF)

# COMMAND ----------

geolocator = Nominatim(user_agent="https://maps.googleapis.com")
def getCoordinatesbyMuni(munic):
    """
    Funcion para obtener posicion lat lon dado un municipio
    """
    try:
        location = geolocator.geocode(munic)
    except Exception as e:
        print(e)
        return None
    return f"{location.raw.get('lat')} {location.raw.get('lon')}"

# COMMAND ----------

from geopy.extra.rate_limiter import RateLimiter
from tqdm import tqdm

from geopy.geocoders import Nominatim

geolocator = Nominatim(user_agent="https://maps.googleapis.com")

missing_muniPD = missing_muni.toPandas()

# Se limpia el campo municipio para una mejor precision de la api de google
missing_muniPD['municipio_clean']=missing_muniPD['municipio'].apply(lambda x: x.split(",")[0])
missing_muniPD['municipio_clean'] = missing_muniPD['municipio_clean']+ ', España'

tqdm.pandas()
missing_muniPD['location_code'] = missing_muniPD['municipio_clean'].progress_apply(getCoordinatesbyMuni)

# COMMAND ----------

missing_muniPD[missing_muniPD['location_code'].isna()]

# COMMAND ----------

#Algunos municipios no se han  podido obtener. Se mapean manualmente
missing_muniPD.loc[missing_muniPD['municipio']=='Port de la Selva, el','location_code']='42.3373159 3.2047232'
# missing_muniPD.loc[missing_muniPD['municipio']=='Pont de Bar, el','location_code'] ='42.3721732 1.6047661'
missing_muniPD.loc[missing_muniPD['municipio']=="Vall d'en Bas, la",'location_code'] ='42.1388600 2.4410500'
# missing_muniPD.loc[missing_muniPD['municipio']=="Ametlla de Mar, l'",'location_code'] ='40.8838957 0.8024645'

# COMMAND ----------

missing_muniDF=spark.createDataFrame(missing_muniPD).select(["municipio","location_code","codigo_municipio"])

missing_muniDF= missing_muniDF.withColumn("municipio_lat", F.split("location_code"," ").getItem(0)) \
.withColumn("municipio_lon", F.split("location_code"," ").getItem(1))
display(missing_muniDF)

# COMMAND ----------

# Se ejecuta una crossjoin para tener todas la posibilidades de estaciones y
#municipios con el fin de calcular la distancia entre todos ellos y finalmente obtener la estacion más cercana

munic_estaciones_convinations = missing_muniDF.crossJoin(estacionesDF)
munic_estaciones_convinations.count()

# COMMAND ----------

from geopy.distance import geodesic
from pyspark.sql.types import FloatType

@F.udf(returnType=FloatType())
def geodesic_udf(a, b):
    """
    Funcion para calcular la distancia entre dos puntos de coordinadas
    """
    return geodesic(a, b).m

estacion_munic_distancia = munic_estaciones_convinations.withColumn('estacion_muni_distancia_m', geodesic_udf(F.array("municipio_lat", "municipio_lon"), F.array("latitud", "longitud")))
display(estacion_munic_distancia)

# COMMAND ----------

#Calculamos un ranking de estaciones mas cercanas a mas lejanas
# La estacion con row_number igual a 1 sera la mas cercana
windowSpec  = Window.partitionBy("municipio").orderBy("estacion_muni_distancia_m")

estacion_munic_distancia=estacion_munic_distancia.withColumn("row_number",row_number().over(windowSpec))
display(estacion_munic_distancia)

# COMMAND ----------

estacion_munic_distancia= estacion_munic_distancia.filter(F.col("row_number")=="1").select(["codigo_municipio","municipio","indicativo","nombre","row_number"]).withColumnRenamed("nombre","nombre_estacion")

# COMMAND ----------

display(estacion_munic_distancia)

# COMMAND ----------

estacion_munic_distancia.write.mode("overwrite").parquet(f"/mnt/IncendiosForestalesCAT/prep/meteocat/estaciones_cercanas_municipio/")