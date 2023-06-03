# Databricks notebook source
!pip install geopy

# COMMAND ----------

import pyspark.sql.functions as F
from functools import reduce

# COMMAND ----------

# MAGIC %md
# MAGIC #METEOCAT

# COMMAND ----------

# MAGIC %md
# MAGIC ##Union de las  Variables y Valores Climatologicos de METEOCAT

# COMMAND ----------

# MAGIC %md
# MAGIC - **Pivot de variablesValoresClimaDF**
# MAGIC 
# MAGIC     Para que cada fila de la variable sea una columna.
# MAGIC 
# MAGIC - **Se renombran las columnas de la tabla pivoteada de METEOCAT para que sean igual que el DF de AEMET**
# MAGIC - **Se añaden el nombre de estación, municipio y la provincia a la tabla Pivoteada.**

# COMMAND ----------

variablesValoresClimaDF = spark.read.format("parquet") \
.load("/mnt/IncendiosForestalesCAT/raw/meteocat/variables")


valoresClimaMeteocatDf = spark.read.format("parquet") \
.load("/mnt/IncendiosForestalesCAT/raw/meteocat/valores_clima_diarios")

variablesValoresClimaDF = variablesValoresClimaDF.alias("vmeteo")\
.join(valoresClimaMeteocatDf.alias("vcmd_meteo"), F.col("vmeteo.codi_variable") == F.col("vcmd_meteo.codi_variable"), "inner")\
.select(["vmeteo.nom_variable", "vcmd_meteo.valor_lectura", "vmeteo.unitat", "vcmd_meteo.codi_estacio", "vcmd_meteo.data_lectura"])

# Pivot
variables = ['Pressió atmosfèrica mínima', 
             'Pressió atmosfèrica', 
             'Pressió atmosfèrica màxima', 
             'Direcció de vent 10 m (m. 1)', 
             'Direcció del vent a 6 m (m. 1)',
             'Direcció del vent a 2 m (m. 1)', 
             'Velocitat del vent a 10 m (esc.)', 
             'Velocitat del vent a 2 m (esc.)', 
             'Velocitat del vent a 6 m (esc.)', 
             'Ratxa màxima del vent a 10 m', 
             'Ratxa màxima del vent a 2 m', 
             'Ratxa màxima del vent a 6 m', 
             'Direcció de la ratxa màxima del vent a 10 m',
             'Direcció de la ratxa màxima del vent a 2 m', 
             'Direcció de la ratxa màxima del vent a 6 m', 
             'Precipitació', 
             'Temperatura', 
             'Temperatura mínima', 
             'Humitat relativa',  
             'Humitat relativa mínima', 
             'Humitat relativa màxima',
             'Precipitació màxima en 1 minut',
             'Irradiància solar global',
             'Irradiància neta',
             'Temperatura màxima',
             'Gruix de neu a terra']

variablesValoresClimaDF = variablesValoresClimaDF.withColumn("valor_lectura", F.col("valor_lectura").cast("float"))

pivotDF = variablesValoresClimaDF.groupBy("data_lectura", "codi_estacio").pivot("nom_variable", variables).sum("valor_lectura")\
.withColumn("velmedia", F.concat_ws("",F.col("`Velocitat del vent a 10 m (esc.)`"), 
                                                     F.col("`Velocitat del vent a 2 m (esc.)`"),
                                                     F.col("`Velocitat del vent a 6 m (esc.)`")).cast("float")) \
                  .withColumn("racha", F.concat_ws("",F.col("`Ratxa màxima del vent a 10 m`"), 
                                 F.col("`Ratxa màxima del vent a 6 m`"),
                                 F.col("`Ratxa màxima del vent a 2 m`")).cast("float"))

rename_columns = {"fecha":"data_lectura",
"prec":"Precipitació",
"presMax": "Pressió atmosfèrica màxima",
"presMin": "Pressió atmosfèrica mínima",
"sol" :"Irradiància solar global",
"tmax": "Temperatura màxima",
"tmed": "Temperatura",
"tmin": "Temperatura mínima",
"indicativo":"codi_estacio",
"rhum":"Humitat relativa",
"velmedia":"velmedia",
"racha":"racha"}

pivotRenamedDF = reduce(lambda dfl, x: dfl.withColumnRenamed(rename_columns[x], x),rename_columns, pivotDF).select(*rename_columns.keys())

columns = ["fecha", "indicativo", "nombre","provincia", "prec", "tmax", "tmed", "tmin", "velmedia", "racha", "sol", "presMax", "presMin", "rhum", "municipio", "cod_municipio"]

estaciones_meteocatDF = spark.read.parquet("/mnt/IncendiosForestalesCAT/raw/meteocat/estaciones/")
pivotRenamedDF = pivotRenamedDF.alias("valoresMeteo").join(estaciones_meteocatDF.alias("eMeteo"), pivotRenamedDF.indicativo ==estaciones_meteocatDF.codi_estacio, "inner" )\
.select("valoresMeteo.*","eMeteo.nom_provincia", "eMeteo.nom_estacio","eMeteo.codi_municipi", "eMeteo.nom_municipi", "eMeteo.codi_provincia") \
.withColumnRenamed("nom_provincia","provincia") \
.withColumnRenamed("nom_estacio","nombre") \
.withColumnRenamed("nom_municipi","municipio") \
.withColumn("cod_municipio",F.concat(F.col("codi_provincia"),F.col("codi_municipi"))) \
.select(columns)

display(pivotRenamedDF)


# COMMAND ----------

display(pivotRenamedDF.filter(F.col("municipio") == "Amposta"))

# COMMAND ----------

# MAGIC %md
# MAGIC # AEMET

# COMMAND ----------

valores_aemetDF = spark.read.parquet("/mnt/IncendiosForestalesCAT/raw/aemet/valores_clima_diarios")\
.filter(F.col("provincia").isin(["BARCELONA", "TARRAGONA", "GIRONA", "LLEIDA"]))

columns = ["fecha", "indicativo", "nombre","provincia", "prec", "tmax", "tmed", "tmin", "velmedia", "racha", "sol", "presMax", "presMin", "rhum", "fuente"]
valores_aemetDF = valores_aemetDF.withColumn("fuente", F.lit("aemet")).withColumn("rhum", F.lit(None)).select(columns)\
.withColumn('prec', F.regexp_replace('prec', ',', '.'))\
.withColumn('tmax', F.regexp_replace('tmax', ',', '.'))\
.withColumn('tmed', F.regexp_replace('tmed', ',', '.'))\
.withColumn('tmin', F.regexp_replace('tmin', ',', '.'))\
.withColumn('velmedia', F.regexp_replace('velmedia', ',', '.'))\
.withColumn('racha', F.regexp_replace('racha', ',', '.'))\
.withColumn('sol', F.regexp_replace('sol', ',', '.'))\
.withColumn('presMax', F.regexp_replace('presMax', ',', '.'))\
.withColumn('presMin', F.regexp_replace('presMin', ',', '.'))

display(valores_aemetDF)

# COMMAND ----------

estaciones_aemetDF = spark.read.parquet("/mnt/IncendiosForestalesCAT/raw/aemet/estaciones")\
.filter(F.col("provincia").isin(["BARCELONA", "TARRAGONA", "GIRONA", "LLEIDA"]))
display(estaciones_aemetDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Añadir cod_municipio INE y municipio a estaciones aemet

# COMMAND ----------

import pandas as pd
estaciones_aemetPD = estaciones_aemetDF.toPandas()

estaciones_aemetPD['municipio']=["Arenys de Mar","Barcelona","Barcelona","Barcelona","Manresa","Sabadell","Santa Susanna","Grirona","Grirona","Grirona","Ripoll","Reus","Vandellos l’Hospitalet de l’Infant","Alp","Seu d'Urgell, La","Lleida","Lleida","Naut aran","Talarn","Tarrega","Roquetes","Sant Jaume d'Enveja"]
estaciones_aemetPD['cod_municipio']=[8006,8019,8019,8019,8113,8187,8261,17079,17079,17079,17147,43123,43162,17006,25203,25119,25119,25025,25215,25217,43133,43902]
estaciones_aemetDF= spark.createDataFrame(estaciones_aemetPD)

# COMMAND ----------

# MAGIC %md
# MAGIC ## JUNTAR VALORES CLIMA DIARIOS AEMET + ESTACIONES AEMET

# COMMAND ----------

valores_aemetDF = valores_aemetDF.alias("vdAemet").join(estaciones_aemetDF.alias("eAemet"), F.col("vdAemet.indicativo")== F.col("eAemet.indicativo"),"inner").select(["vdAemet.*", "eAemet.municipio","eAemet.cod_municipio"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## JUNTAR VALORES METEO AEMET + METEOCAT

# COMMAND ----------

# MAGIC %md
# MAGIC **Se unen los valores de AEMET y METEOCAT y se crea un campo "Fuente", en caso de querer conocer a que fuente corresponde**

# COMMAND ----------

columns = ["fecha", "indicativo", "nombre","provincia", "prec", "tmax", "tmed", "tmin", "velmedia", "racha", "sol", "presMax", "presMin", "rhum", "fuente", "municipio", "cod_municipio"]
valores_aemet_meteocatDF = valores_aemetDF.union(pivotRenamedDF.withColumn("fuente", F.lit("meteocat")).select(columns))
valores_aemet_meteocatDF = reduce(lambda dfl, x: dfl.withColumn(x[0], F.col(x[0]).cast(x[1])), pivotRenamedDF.dtypes, valores_aemet_meteocatDF)

# COMMAND ----------

estaciones_meteocatDF.count()

# COMMAND ----------

pivotDF.count()

# COMMAND ----------

pivotRenamedDF.count()

# COMMAND ----------

valores_aemetDF.count()

# COMMAND ----------

valores_aemet_meteocatDF.count()

# COMMAND ----------

valores_aemet_meteocatDF.write.mode("overwrite").parquet(f"/mnt/IncendiosForestalesCAT/prep/aemet_meteocat/valores_clima_diarios/")

# COMMAND ----------

display(valores_aemet_meteocatDF)