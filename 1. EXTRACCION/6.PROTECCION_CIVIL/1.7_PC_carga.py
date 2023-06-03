# Databricks notebook source
from functools import reduce
import pyspark.sql.functions as F

# COMMAND ----------

display(dbutils.fs.ls(f"/mnt/IncendiosForestalesCAT/raw/proteccion_civil_cat"))

# COMMAND ----------

# MAGIC %md
# MAGIC #PROTECCION CIVIL CATALUÑA

# COMMAND ----------

#CSV
Riesgo_incendio_PC_CAT = spark.read.format("csv") \
.option("inferSchema", "true") \
.option("header", "true") \
.option("sep", ",") \
.load("/mnt/IncendiosForestalesCAT/raw/proteccion_civil_cat/incendios_riesgo.csv")

# COMMAND ----------

display(Riesgo_incendio_PC_CAT)

# COMMAND ----------

Riesgo_incendio_PC_CAT.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ##JOIN ALTITUD y PENDIENTE

# COMMAND ----------

#CSV
alt_pend = spark.read.format("parquet") \
.load("/mnt/IncendiosForestalesCAT/prep/idescat/cartografia/")

# COMMAND ----------

alt_pend_risc = alt_pend.alias("df1").join(Riesgo_incendio_PC_CAT.alias("df2"), F.col("df1.cod_municipio")== F.col("df2.MUNICIPI"), "left").select(["df1.*","df2.MUNICIPI","PERILL", "NOM_MUNI"])

# COMMAND ----------

alt_pend_risc.count()

# COMMAND ----------

display(alt_pend_risc)