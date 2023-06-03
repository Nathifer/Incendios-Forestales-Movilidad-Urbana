# Databricks notebook source
from functools import reduce

def load_table(file_location, 
               table_name, 
               rename_columns=None, 
               file_type="csv", 
               delimiter=";"):
  
  # The applied options are for CSV files. For other file types, these will be ignored.
  df = spark.read.format(file_type) \
    .option("inferSchema", "true") \
    .option("header", "true") \
    .option("sep", delimiter) \
    .load(file_location)
  df.columns
  if rename_columns is not None:
    df = reduce(lambda dfl, x: dfl.withColumnRenamed(x,rename_columns[x]),rename_columns, df)
    df.columns

  # Create a view or table
  df.createOrReplaceTempView(table_name)

  # Since this table is registered as a temp view, it will only be available to this notebook. If you'd like other users to be able to query  this table, you can also create a table from the DataFrame.
  # Once saved, this table will persist across cluster restarts as well as allow various users across different notebooks to query this data.
  # To do so, choose your table name and uncomment the bottom line.

  df.write.format("parquet").saveAsTable(table_name)


# COMMAND ----------

# MAGIC %md
# MAGIC # INE

# COMMAND ----------

# MAGIC %md
# MAGIC ### Codigo de Municipios - CODIGO MITMA

# COMMAND ----------

# TABLA DEL INE CODIGO DE MUNCIPIOS - CODIGO MITMA
file_location = "/mnt/IncendiosForestalesCAT/raw/ine/"
file_type = "csv"
table_name = "nacional_moviles"

load_table(file_location, table_name)

# COMMAND ----------

municipios_catalunya_INE = spark.sql("""
SELECT * FROM nacional_moviles WHERE NPRO in ('Barcelona', 'Tarragona', 'Girona', 'Lleida');
""")
display(municipios_catalunya_INE)

# COMMAND ----------

municipios_catalunya_INE = spark.sql("""
SELECT * FROM nacional_moviles WHERE NPRO in ('Barcelona', 'Tarragona', 'Girona', 'Lleida') and CONTAINS(NMUN, 'Tàrrega') ;
""")
display(municipios_catalunya_INE)

# COMMAND ----------

# MAGIC %md
# MAGIC ### CCAA_PROVINCIAS

# COMMAND ----------

# MAGIC %md
# MAGIC **Datos disponibles en:** https://www.ine.es/daco/daco42/codmun/cod_ccaa_provincia.htm

# COMMAND ----------

# TABLA DEL INE - CCAA_PROVINCIAS
file_location = "/mnt/IncendiosForestalesCAT/raw/ine/provincias-espanolas_nathalia.csv"
permanent_table_name = "cca_provincias"
dbutils.fs.rm(f"/user/hive/warehouse/{permanent_table_name}/", True)

load_table(file_location, permanent_table_name,rename_columns={"CODAUTO":"cod_autonoma","Comunidad Autónoma":"comunidad_autonoma","CPRO":"cod_provincia","Provincia":"provincia"}, delimiter=",")

# COMMAND ----------

# MAGIC %sql SELECT * FROM cca_provincias where comunidad_autonoma = "Cataluña" ;