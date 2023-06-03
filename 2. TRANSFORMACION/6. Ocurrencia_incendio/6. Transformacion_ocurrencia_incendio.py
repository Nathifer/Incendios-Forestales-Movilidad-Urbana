# Databricks notebook source
!pip install geopy tqdm

# COMMAND ----------

from functools import reduce
import pyspark.sql.functions as F

# COMMAND ----------

display(dbutils.fs.ls(f"/mnt/IncendiosForestalesCAT/raw/meteocat/"))

# COMMAND ----------

#CSV
incendios_ocurridos_CAT = spark.read.format("parquet") \
.load("/mnt/IncendiosForestalesCAT/raw/meteocat/incendios_forestales")

# COMMAND ----------

display(incendios_ocurridos_CAT)

# COMMAND ----------

# Using Cast to convert Timestamp String to DateType
incendios_ocurridos_CAT = incendios_ocurridos_CAT.withColumn('fecha', F.split(F.col('data_incendi'),'T').getItem(0))

# COMMAND ----------

display(incendios_ocurridos_CAT)

# COMMAND ----------

incendios_ocurridos_CAT.count()

# COMMAND ----------

incendios_ocurridos_CAT.write.mode("overwrite").parquet(f"/mnt/IncendiosForestalesCAT/prep/incendios_forestales")
display(incendios_ocurridos_CAT)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Actualmente hay 741 municipios donde hubo incendio y solo tenemos 233 estaciones metorológicas. Por lo que si hacemos join de incencios con valores meteorológicos perderemos muchos datos sobre incendios. 
# MAGIC #### El objetivo es tratar de asignar la estacion meteorológica XEMA más cercana  a cada municicipio.

# COMMAND ----------

# MAGIC %md
# MAGIC ##JOIN OCURRENCIA DE INCENDIOS CON ESTACIONES_MUNIC_DISTANCIA

# COMMAND ----------

#CSV
estacion_munic_distancia = spark.read.format("parquet") \
.load("/mnt/IncendiosForestalesCAT/prep/meteocat/estaciones_cercanas_municipio/")

# COMMAND ----------

estaciones_incendio = incendios_ocurridos_CAT.alias("df1").join(estacion_munic_distancia.alias("df2").filter(F.col("row_number") == 1), (F.col("df1.codi_municipi")== F.col("df2.codigo_municipio")) , "inner").select(["df2.indicativo", "df1.fecha","codi_comarca","comarca","haforestal","df1.codi_municipi"])


# COMMAND ----------

estaciones_incendio.count()

# COMMAND ----------

display(estaciones_incendio)

# COMMAND ----------

# MAGIC %md
# MAGIC ##JOIN VALORES CLIMA CON ESTACIONES_INCENDIO

# COMMAND ----------

#CSV
VC_diarios = spark.read.format("parquet") \
.load("/mnt/IncendiosForestalesCAT/prep/aemet_meteocat/valores_clima_diarios/")

# COMMAND ----------

VC_diarios.count()

# COMMAND ----------

display(VC_diarios)

# COMMAND ----------

# Using Cast to convert Timestamp String to DateType
VC_diarios = VC_diarios.withColumn('fecha', F.split(F.col('fecha'),'T').getItem(0))

# COMMAND ----------

vc_ocurrencia = estaciones_incendio.alias("df1").join(VC_diarios.alias("df2"), (F.col("df1.indicativo")== F.col("df2.indicativo")) & (F.col("df1.fecha")== F.col("df2.fecha")) , "right").select(["df2.*",  "df1.codi_comarca", "df1.comarca", "df1.haforestal", "df1.codi_municipi"]).dropDuplicates(["fecha","indicativo"])

# COMMAND ----------

display(vc_ocurrencia)

# COMMAND ----------

display(vc_ocurrencia.filter(F.col("haforestal").isNotNull()))

# COMMAND ----------

# MAGIC %md
# MAGIC #VARIABLE OCURRENCIA

# COMMAND ----------

from pyspark.sql.functions import when,col
vc_ocurrencia = vc_ocurrencia.withColumn("ocurrencia", when(vc_ocurrencia.haforestal > 0.000, 1).otherwise(0))

# COMMAND ----------

display(vc_ocurrencia)

# COMMAND ----------

vc_ocurrencia.filter(F.col("ocurrencia").contains(1)).count()

# COMMAND ----------

vc_ocurrencia.write.mode("overwrite").parquet(f"/mnt/IncendiosForestalesCAT/prep/vc_ocurrencia")
display(vc_ocurrencia)

# COMMAND ----------

display(dbutils.fs.ls(f"/mnt/IncendiosForestalesCAT/prep/"))

# COMMAND ----------

d_= spark.read.parquet(f"/mnt/IncendiosForestalesCAT/prep/vc_ocurrencia/")
d_.count()