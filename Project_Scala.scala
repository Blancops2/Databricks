// Databricks notebook source
import org.apache.spark.sql.functions._

// COMMAND ----------

val filepath : String = "dbfs:/FileStore/bird_strikes.csv"

// COMMAND ----------

val df = spark.read.option("header","true").option("inferSchema","true").csv(filepath)

// COMMAND ----------

display(
  df
)

// COMMAND ----------


//obtenes la aeronave con mayor cantidad de accidentes

val mayor_accidente = df
  .groupBy("aircraft_type")
  .agg(count("*").as("conteo_accidentes"))
  .orderBy(col("conteo_accidentes").desc)
  .limit(3) //para obtener los primeros 3

  //porque un top 3?:: porque hay columnas que no tienen nada que ver con aeronave


// COMMAND ----------

display(
  mayor_accidente
)

// COMMAND ----------

///aeropuerto menos seguro
val menos_seguro = df
.groupBy("airport_name")
.agg(count("*").as("conteo"))
.orderBy(col("conteo").desc)

// COMMAND ----------

display(
  menos_seguro
)

// COMMAND ----------

////promedio de costo de los accidentes
val promedio = df
.groupBy("aircraft_make_model")
.agg(avg("cost_total").alias("el_promedio"))
.where (col("el_promedio")>0)



// COMMAND ----------

display(
  promedio
)

// COMMAND ----------

//accidentes con costo total mayor al promedio
val promedioGlobal = df
.agg(avg("cost_total"))
.first().getDouble(0)

val mayor_promedio = df
.select(col("aircraft_make_model"), col("cost_total"))
.where(col("cost_total") > promedioGlobal)

// COMMAND ----------

display(
  mayor_promedio
)

// COMMAND ----------

//accidentes reportandos entre 2000 y 2004
val reporte = df
  .filter(col("flightdate").between("2000-01-01T00:00:00", "2004-12-31T23:59:59"))
  .groupBy("flightdate")
  .agg(count("*").as("num_accidentes"))
  .orderBy(col("flightdate").desc)

// COMMAND ----------

display(
  reporte
)

// COMMAND ----------

//accidentes agrupados por años

val anios = df
.select(year(col("flightdate")).alias("año"))
.groupBy("año")
.agg(count("*").alias("conteo"))
.orderBy(col("año").desc)

// COMMAND ----------

display(
  anios
)

// COMMAND ----------

//conteo de modelos de aviones
val altura = df
  .groupBy("aircraft_make_model")
  .agg(
    count("*").alias("conteo"),
    collect_list("airport_name").alias("airport_names")
  )
  .orderBy(col("conteo").desc)

// COMMAND ----------

display(
  altura
)

// COMMAND ----------

//personas lesionadas por año
val lesionadas = df
.select(year(col("flightdate")).alias("año"),col("number_of_people_injured"))
.groupBy("año")
.agg(sum("number_of_people_injured").alias("P_lesionadas"))
.orderBy(col("año").asc)

// COMMAND ----------

display(
  lesionadas
)

// COMMAND ----------

val p_por_anios = df
.select(
    col("airport_name"),
    col("aircraft_type"),
    col("number_of_people_injured"),
    year(col("flightdate")).alias("año")
    )
.groupBy("año")
.agg(max("number_of_people_injured").alias("mayor_num_P"))

// COMMAND ----------

display(
  p_por_anios
)
