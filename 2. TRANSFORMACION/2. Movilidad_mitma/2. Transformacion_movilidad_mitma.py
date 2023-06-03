# Databricks notebook source
#Paquetes
from pyspark.sql.functions import *
import pyspark.sql.functions as F
from functools import reduce

# COMMAND ----------

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

#RAW DATA
df_movilidad = spark.read.format("parquet") \
.load("/mnt/IncendiosForestalesCAT/raw/mitma/movilidad/maestra1/municipios")
display(df_movilidad)

# COMMAND ----------

# MAGIC %md
# MAGIC # Relación municipios MITMA - Tablas INE

# COMMAND ----------

# Relacion Municipio - Municipio mitma
file_location = "/mnt/IncendiosForestalesCAT/raw/mitma/rl_municipio_mitma/"
permanent_table_name = "rl_municipio_mitma"
dbutils.fs.rm(f"/user/hive/warehouse/{permanent_table_name}/", True)

load_table(file_location, permanent_table_name,delimiter="|")

# COMMAND ----------

# MAGIC %md
# MAGIC # INE
# MAGIC ### Municipios INE

# COMMAND ----------

# TABLA DEL INE CODIGO DE MUNCIPIOS - CODIGO MITMA
dbutils.fs.rm("/user/hive/warehouse/nacional_moviles/", True)
file_location = "/mnt/IncendiosForestalesCAT/raw/mitma/nacional_moviles/"
file_type = "csv"
table_name = "nacional_moviles"

load_table(file_location, table_name)

# COMMAND ----------

municipios_catalunya_INE = spark.sql("""
SELECT * FROM nacional_moviles WHERE NPRO in ('Barcelona', 'Tarragona', 'Girona', 'Lleida');
""")

# COMMAND ----------

display(municipios_catalunya_INE)

# COMMAND ----------

municipiosDF = spark.sql("""
select nm.CUMUN as cod_mun_ine, nm.ID_AREA_GEO as cod_area_ine, mm.municipio as cod_mun_mitma_movilidad ,nm.NMUN as nombre_municipio, nm.CPRO as cod_provincia, nm.NPRO as nombre_provincia, mm.municipio_mitma
FROM nacional_moviles as nm
JOIN rl_municipio_mitma as mm ON  nm.CUMUN = mm.municipio
WHERE nm.CUMUN == mm.municipio AND NPRO in ("Barcelona", "Lleida", "Tarragona", "Girona")""")

display(municipiosDF)

# COMMAND ----------

#Join de las tablas  df_movilidad  y municipiosDF
df_movilidad =df_movilidad.join(municipiosDF,df_movilidad.destino ==  municipiosDF.municipio_mitma,"inner")
df_movilidad.count()

# COMMAND ----------

from pyspark.sql import Window
windowSpecAgg  = Window.partitionBy("fecha", "destino")

df_movilidad_agg = df_movilidad.withColumn("total_viajes", F.sum(F.col("viajes")).over(windowSpecAgg)) \
.withColumn("total_viajes_km", F.sum(F.col("viajes_km")).over(windowSpecAgg))
df_movilidad_agg_unique = df_movilidad_agg.dropDuplicates(["fecha", "destino"])
df_movilidad_agg_unique.persist()

#Change format
df_movilidad_agg_unique= df_movilidad_agg_unique\
.withColumn("fecha", F.to_date(F.col("fecha").cast("string"),'yyyy-MM-dd'))\
.withColumn('fecha', F.col("fecha").cast("string"))\
.withColumn("cod_mun_destino", F.col("cod_mun_destino").cast("integer"))


#Write to S3
columns = ['fecha', 'year', 'month' ,'destino', 'cod_mun_destino', 'cod_mun_ine', 'nombre_municipio', 'cod_provincia', 'nombre_provincia', 'total_viajes', 'total_viajes_km']
df_movilidad_agg_unique.select(columns).write.mode("overwrite").partitionBy("year","month").parquet(f"/mnt/IncendiosForestalesCAT/prep/mitma/movilidad/")

# COMMAND ----------

display(df_movilidad_agg_unique)