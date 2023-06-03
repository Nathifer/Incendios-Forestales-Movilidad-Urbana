# Databricks notebook source
from functools import reduce
import pyspark.sql.functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC #GENCAT
# MAGIC ##RIESGO DE INCENDIO

# COMMAND ----------

#CSV
Riesgo_incendio_CAT = spark.read.format("csv") \
.option("inferSchema", "true") \
.option("header", "true") \
.option("sep", ",") \
.load("/mnt/IncendiosForestalesCAT/raw/gencat/riesgo_incendio/Riesgo_incendio_CAT.csv")

# COMMAND ----------

Riesgo_incendio_CAT.count()

# COMMAND ----------

display(Riesgo_incendio_CAT)

# COMMAND ----------

Riesgo_incendio_CAT = Riesgo_incendio_CAT.withColumnRenamed("COMARCA","cod_comarca")

# COMMAND ----------

# MAGIC %md
# MAGIC #PROTECCION CIVIL
# MAGIC ##PELIGRO DE INCENDIO

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

# MAGIC %md
# MAGIC #JOIN GENCAT y PROTECCION CIVIL CATALUÑA

# COMMAND ----------

Riesgo_incendio = Riesgo_incendio_CAT.alias("df1").join(Riesgo_incendio_PC_CAT.alias("df2"), F.col("df1.MUNICIPI")== F.col("df2.MUNICIPI"), "inner").select(["df1.MUNICIPI", "df1.NOM_MUNI","ALT_RISC","COD_INE", "cod_comarca", "NOM_COMAR","PERILL", "VULNER"]).distinct()

# COMMAND ----------

display(Riesgo_incendio)

# COMMAND ----------

# MAGIC %md
# MAGIC #JOIN ALTITUD Y PENDIENTE CON RIESGO DE INCENDIO

# COMMAND ----------

#CSV
alt_pend = spark.read.format("parquet") \
.load("/mnt/IncendiosForestalesCAT/prep/idescat/cartografia/")

# COMMAND ----------

display(alt_pend)

# COMMAND ----------

alt_pend_risc = alt_pend.alias("df1").join(Riesgo_incendio.alias("df2"), F.col("df1.cod_idescat")== F.col("df2.MUNICIPI"), "inner").select(["df1.*","df2.cod_comarca","COD_INE","ALT_RISC","NOM_COMAR", "MUNICIPI", "NOM_MUNI", "PERILL", "VULNER"])

# COMMAND ----------

display(alt_pend_risc)

# COMMAND ----------

alt_pend_risc.write.mode("overwrite").parquet(f"/mnt/IncendiosForestalesCAT/prep/alt_pend_risc")
display(alt_pend_risc)