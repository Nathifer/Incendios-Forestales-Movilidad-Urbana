# Databricks notebook source
# MAGIC %md
# MAGIC Union de valores clima, altitud, pendiente, ocurrencia de incendio, alto riesgo,  peligro y vulnerabilidad con datos de movilidad urbana

# COMMAND ----------

import pyspark.sql.functions as F

# COMMAND ----------

movilidadDF = spark.read.parquet('/mnt/IncendiosForestalesCAT/prep/mitma/movilidad/')
df = spark.read.parquet(f"/mnt/IncendiosForestalesCAT/prep/riesgo_incendios_catalunya_all")
estacionesMunicipioDF = spark.read.parquet(f"/mnt/IncendiosForestalesCAT/prep/meteocat/estaciones_cercanas_municipio/")

# COMMAND ----------

display(movilidadDF)

# COMMAND ----------

display(df)

# COMMAND ----------

display(estacionesMunicipioDF)

# COMMAND ----------

estacionesMunicipioDF = estacionesMunicipioDF.withColumn("codigo_municipio_m",F.expr("substring(codigo_municipio, 1, length(codigo_municipio)-1)"))

# COMMAND ----------

df.count()

# COMMAND ----------

movilidadDF = movilidadDF.alias("df1").join(estacionesMunicipioDF.alias("df2"), F.col("df1.cod_mun_ine")==F.col("df2.codigo_municipio_m"),"inner").select(["df1.fecha","df2.indicativo","df1.total_viajes","df1.total_viajes_km"])


# COMMAND ----------

movilidadDF.count()

# COMMAND ----------

df_all = df.alias("df1").join(movilidadDF.alias("df2"), (F.col("df1.indicativo")== F.col("df2.indicativo")) & (F.col("df1.fecha")== F.col("df2.fecha")), "left").select(["df1.*","df2.total_viajes","df2.total_viajes_km"])
df_all.count()

# COMMAND ----------

display(df_all)

# COMMAND ----------

df_all.coalesce(1).write.mode("overwrite").option("header",True).csv(f"/mnt/IncendiosForestalesCAT/prep/riesgo_incendios_catalunya_all_movilidad/")
