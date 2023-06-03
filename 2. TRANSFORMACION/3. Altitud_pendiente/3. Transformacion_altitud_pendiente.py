# Databricks notebook source
from functools import reduce
import pyspark.sql.functions as F

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

# MAGIC %md
# MAGIC #ALTITUD

# COMMAND ----------

altitudDF = spark.read.format("csv") \
.option("inferSchema", "false") \
.option("header", "true") \
.option("sep", ";") \
.load("/mnt/IncendiosForestalesCAT/raw/idescat/cartografia/Altitud.csv")
display(altitudDF)

# COMMAND ----------

rename_columns = {"Altitud (m)":"altitud",
"Codigo":"cod_idescat",
"Municipio": "municipio",
"Comarca": "comarca"
}

altitudDF = reduce(lambda dfl, x: dfl.withColumnRenamed(x,rename_columns[x]),rename_columns, altitudDF).select(*rename_columns.values())
altitudDF = altitudDF.withColumn("cod_municipio", F.substring("cod_idescat",1,5))

# COMMAND ----------

display(altitudDF)

# COMMAND ----------

display(altitudDF.filter(F.col("comarca")=="Vallès Occidental"))

# COMMAND ----------

# MAGIC %md
# MAGIC #PENDIENTE

# COMMAND ----------

#CSV
pendienteDF = spark.read.format("csv") \
.option("inferSchema", "false") \
.option("header", "true") \
.option("sep", "\t") \
.load("/mnt/IncendiosForestalesCAT/raw/idescat/cartografia/pendienteCAT.csv")
display(pendienteDF)

# COMMAND ----------

rename_columns={"Municipio":"comarca","Superfície total":"superficie_total","Superfície amb pendent < 20%":"superficie_menos_20","% pendent < 20%":"porcentaje_pendiente", "% pendent < 20% sobre el total":"porcentaje_pendiente_total"}

pendienteDF = reduce(lambda dfl, x: dfl.withColumnRenamed(x,rename_columns[x]),rename_columns, pendienteDF).select(*rename_columns.values())
display(pendienteDF)

# COMMAND ----------

# MAGIC %md
# MAGIC #JOIN ALTITUD y PENDIENTE

# COMMAND ----------

altitudDF.count()

# COMMAND ----------

pendienteDF.count()

# COMMAND ----------

cartografiaDF = altitudDF.alias("df1").join(pendienteDF.alias("df2"), F.col("df1.comarca")== F.col("df2.comarca"), "left").select(["df1.*","df2.superficie_total","superficie_menos_20","porcentaje_pendiente","porcentaje_pendiente_total"])

# COMMAND ----------

display(cartografiaDF)

# COMMAND ----------

cartografiaDF.write.mode("overwrite").parquet(f"/mnt/IncendiosForestalesCAT/prep/idescat/cartografia/")