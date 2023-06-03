# Databricks notebook source
# MAGIC %md
# MAGIC # IDESCAT

# COMMAND ----------

# MAGIC %md
# MAGIC ### Altitud

# COMMAND ----------

# MAGIC %md
# MAGIC **Datos disponibles en:** https://www.idescat.cat/indicadors/?id=aec&n=15903&lang=es

# COMMAND ----------

Altitud = spark.read.format("csv") \
.option("inferSchema", "true") \
.option("header", "true") \
.option("sep", ";") \
.load("/mnt/IncendiosForestalesCAT/raw/idescat/cartografia/Altitud.csv")

# COMMAND ----------

Altitud.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Superficie y Pendientes

# COMMAND ----------

# MAGIC %md
# MAGIC **Datos disponibles en:** https://www.idescat.cat/indicadors/?id=aec&n=15181&lang=es
# MAGIC 
# MAGIC 
# MAGIC + Unidades: Kilómetros cuadrados.
# MAGIC + Fuente: Institut Cartogràfic i Geològic de Catalunya.

# COMMAND ----------

#CSV
Pendiente = spark.read.format("csv") \
.option("inferSchema", "true") \
.option("header", "true") \
.option("sep", "\t") \
.load("/mnt/IncendiosForestalesCAT/raw/idescat/cartografia/pendienteCAT.csv")

# COMMAND ----------

Pendiente.display()