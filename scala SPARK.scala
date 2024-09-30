// Databricks notebook source
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame


// COMMAND ----------

// SELECCION DE PATH PARA TRABAJAR CON MULTIPLES CSV CON POO

class NDataframe(spark: SparkSession, inputPath: String) {

  // Método para leer los datos de un archivo CSV
  def readData(): DataFrame = {
    val df = spark.read.format("csv").option("header", "true").load(path)
    df
  }}


    // Crear una sesión de Spark
val spark = SparkSession.builder()
.appName("Daniel session")
       // Habilitar soporte para Hive
.getOrCreate()

val path = "dbfs:/FileStore/shared_uploads/hevert_snakerd_095@hotmail.com/ventas_totales_supermercados_2.csv"

val datos = new NDataframe(spark, path)
val df = datos.readData()


// COMMAND ----------

df.printSchema

// COMMAND ----------

// OPERARIONES Y FILTRADO DE COLUMNAS

val df2 = df.select("canales_on_line", "panaderia")

// COMMAND ----------

df2.withColumn("suma", $"canales_on_line" + $"panaderia").show()

// COMMAND ----------

df2.filter($"canales_on_line" < 300000 && $"panaderia" < 1000000).show()

// COMMAND ----------

df2.orderBy($"canales_on_line".desc).show()

// COMMAND ----------

df2.select($"panaderia").show()

// COMMAND ----------

df2.select($"panaderia" / 1000).show()

// COMMAND ----------

// CAST DE COLUMNA a DATE


var df_g=df.withColumn("fecha",col("indice_tiempo").cast("date"))
df_g.printSchema()

// COMMAND ----------

val df_g2 = df_g.withColumn("anio", year($"fecha"))

// COMMAND ----------

// GROUPBY MAS OPERACIONES CORRESPONDIENTES

val df0=df_g2.select("anio", "bebidas", "almacen", "panaderia", "lacteos", "carnes")
df0.groupBy("anio").agg(sum("bebidas"),sum("almacen"), sum("panaderia"), sum("lacteos"), sum("carnes")).show()

// COMMAND ----------

// OPERACION ANTERIOS CON FOR 

for (columns <- df0.columns) {
    df0.groupBy("anio").agg(sum(columns)).show()
}

// COMMAND ----------
// USO DE udf y When y Otherwise

val cambionombre = udf((panaderia: Int) => if (panaderia < 5000000) "Barato" else "Caro")
val pan =df2.withColumn("panaderia Clasificacion", cambionombre($"panaderia"))

// COMMAND ----------

pan.groupBy("panaderia Clasificacion").count().show()

// COMMAND ----------

val pan2=pan.withColumn("When oth", when($"panaderia Clasificacion" === "Barato", "Pan defectuoso").otherwise("Buena Calidad"))
pan2.show()

// COMMAND ----------

pan2.groupBy("When oth").count().show()

// COMMAND ----------

//Uso de Window

import org.apache.spark.sql.expressions.Window

val windowSpec = Window.partitionBy("anio").orderBy("panaderia")
val window = df0.withColumn("Orden", row_number().over(windowSpec))
window.show()

// COMMAND ----------

val fil =window.filter($"Orden" === 12)
fil.show()

// COMMAND ----------

fil.withColumn("Fecha hoy", current_date).show()

// COMMAND ----------

//Creacion de vista temporal

fil.createOrReplaceTempView("Fechahoy")

// COMMAND ----------

spark.sql("SELECT * FROM Fechahoy WHERE anio == 2020").show()

// COMMAND ----------

fil.selectExpr("avg(panaderia)").show()

// COMMAND ----------

fil.selectExpr("Orden * 1000").show()

// COMMAND ----------

fil.printSchema()

// COMMAND ----------

// Cast de todas las columnas con SelectExpr

val sel_tip=fil.selectExpr("ANIO", "CAST (bebidas AS FLOAT)", "CAST (bebidas AS FLOAT)", "CAST(almacen AS FLOAT)", "CAST (panaderia AS FLOAT)", "CAST (lacteos AS FLOAT)", "CAST(carnes AS FLOAT)", "Orden")

// COMMAND ----------

sel_tip.printSchema()

// COMMAND ----------

val table = "database.DANIEL"

// COMMAND ----------

// Creacion de tabla en SQL

spark.sql("""" CREATE TABLE DANIEL IF NOT EXISTS (
  ANIO INT,
  bebidas FLOAT,
  almacen FLOAT,
  panaderia FLOAT,
  lacteos FLOAT,
  carnes FLOAT,
  Orden INT
) STORED AS PARQUET """")

// COMMAND ----------


df.write.mode("append").insertInto(table)
  
