# Databricks notebook source
from functools import reduce
import pyspark.sql.functions as F

# COMMAND ----------

display(dbutils.fs.ls(f"/mnt/IncendiosForestalesCAT/prep/"))

# COMMAND ----------

#CSV
vc_ocurrencia = spark.read.format("parquet") \
.load("/mnt/IncendiosForestalesCAT/prep/vc_ocurrencia")

# COMMAND ----------

vc_ocurrencia.count()

# COMMAND ----------

#CSV
alt_pend_risc = spark.read.format("parquet") \
.load("/mnt/IncendiosForestalesCAT/prep/alt_pend_risc")

#CSV
cod_estacion_municDF = spark.read.format("parquet") \
.load("/mnt/IncendiosForestalesCAT/prep/meteocat/estaciones_cercanas_municipio/").filter(F.col("row_number")=="1")

# COMMAND ----------

display(cod_estacion_municDF)

# COMMAND ----------

vc_ocurrencia.count()

# COMMAND ----------

alt_pend_risc.count()

# COMMAND ----------

alt_pend_risc_estacionDF = alt_pend_risc.alias("df1").join(cod_estacion_municDF.alias("df2"), (F.col("df1.cod_idescat")== F.col("df2.codigo_municipio")), "inner").select(["df1.*", "df2.indicativo"])
display(alt_pend_risc_estacionDF)

# COMMAND ----------

display(alt_pend_risc_estacionDF.groupBy("indicativo").count().filter("count > 1"))

# COMMAND ----------

display(alt_pend_risc_estacionDF.filter(F.col("indicativo")=="XU"))

# COMMAND ----------

alt_pend_risc_estacionDF.count()

# COMMAND ----------

Rif_cat = alt_pend_risc_estacionDF.alias("df1").join(vc_ocurrencia.alias("df2"), (F.col("df1.indicativo")== F.col("df2.indicativo")), "inner").select(["df2.*", "df1.altitud", "df1.porcentaje_pendiente_total", "ALT_RISC", "PERILL", "VULNER"])

# COMMAND ----------

Rif_cat.count()

# COMMAND ----------

display(Rif_cat.filter((F.col("indicativo")=="XU") & (F.col("fecha")=="2022-01-29")))

# COMMAND ----------

Rif_cat = Rif_cat.withColumnRenamed("nombre","nombre_estacion") \
.withColumnRenamed("NOM_MUNI","nombre_municipio") \
.withColumnRenamed("COD_INE","codi_ine") \
.withColumnRenamed("ALT_RISC","alto_riesgo") \
.withColumnRenamed("VULNER","vulner") \
.withColumnRenamed("PERILL","peligro") \
.withColumnRenamed("NOM_COMAR","nombre_comarca") \
.withColumnRenamed("MUNICIPI","municipio_riesgo") 

# COMMAND ----------

display(Rif_cat)

# COMMAND ----------

Rif_cat = Rif_cat.select("fecha", "prec", "tmax", "tmed", "tmin","velmedia","racha","sol","presMax","presMin","rhum","altitud","porcentaje_pendiente_total", "alto_riesgo","peligro","vulner", "ocurrencia", "df2.municipio","df2.indicativo" ,"df2.cod_municipio")

# COMMAND ----------

display(Rif_cat)

# COMMAND ----------

Rif_cat.write.mode("overwrite").parquet(f"/mnt/IncendiosForestalesCAT/prep/riesgo_incendios_catalunya_all")
display(Rif_cat)