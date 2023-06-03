# Databricks notebook source
# MAGIC %md
# MAGIC # Agricultura Gencat

# COMMAND ----------

# MAGIC %md
# MAGIC ### Municipios con alto riesgo de incendio forestal

# COMMAND ----------

# MAGIC %md
# MAGIC **Datos disponibles en:**  https://agricultura.gencat.cat/es/serveis/cartografia-sig/bases-cartografiques/boscos/municipis-alt-risc-incendi-forestal/
# MAGIC 
# MAGIC Nota: estos datos se han extraido de la tabla de atributos disponible dentro del fichero SHP, mediante la herramienta Qgis. 

# COMMAND ----------

#CSV
Riesgo_incendio_CAT = spark.read.format("csv") \
.option("inferSchema", "true") \
.option("header", "true") \
.option("sep", ",") \
.load("/mnt/IncendiosForestalesCAT/raw/gencat/riesgo_incendio/Riesgo_incendio_CAT.csv")

# COMMAND ----------

Riesgo_incendio_CAT.display()

# COMMAND ----------

