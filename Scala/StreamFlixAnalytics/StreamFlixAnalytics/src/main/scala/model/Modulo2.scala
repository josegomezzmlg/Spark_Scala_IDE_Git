package com.streamflix
package model

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

object Modulo2 {
  def ejecutar(): Unit = {
    val spark = SparkSession.builder()
      .appName("StreamFlix")
      .master("local[*]")
      .getOrCreate()
    // Si no pongo el SetLogLevel me aparecen todas las lineas de INFO
    spark.sparkContext.setLogLevel("ERROR")

    //Tarea:
    //  Cargar un CSV de catálogo de películas (movies_metadata.csv). El archivo tiene precios en
    //formato string con símbolos de moneda (ej: "$12.99") y géneros separados por pipes |. Debes
    //limpiarlo y tiparlo correctamente.
    //Dataset (movies_metadata.csv):

    val customSchema = StructType(Array(
      StructField("id", LongType, nullable = false),
      StructField("title", StringType, nullable = false),
      StructField("genres",StringType, nullable = false),
      StructField("subscription_price", StringType,nullable = false),
      StructField("release_date", StringType,nullable = false),
      StructField("country",StringType,nullable = false)))

    val dfMovies = spark.read
      .option("header", "true")
      .schema(customSchema)
      .csv("src/main/resources/data/movies_metadata.csv")

    // TODO: Crear una UDF o usar expresiones select para limpiar el precio
    //  (quitar '$' y castear a Double)

    val dfClean = dfMovies.withColumn("subscription_price",
      regexp_replace(col("subscription_price"),"\\$","").cast("decimal(10,2)")
    )
    val dfClean2 = dfClean.withColumn("genres",
      regexp_replace(col("genres"),"\\|",","))

    dfClean2.show()
    dfClean2.printSchema()

    // TODO: Hacer un análisis de nulos y duplicados en las columnas
    println("- Análisis de nulos y duplicados")
    val dfNulos = dfClean2.filter(
      dfClean2.columns
        .map(c => col(c).isNull)
        .reduce((cond1, cond2) => cond1 || cond2)
    )

    dfNulos.show(5)
    println("Hay "+ dfNulos.count()+" lineas con valores nulos")

    val df_duplicados = dfClean2.groupBy(dfClean2.columns.map(col): _*)
      .count()
      .filter(col("count") > 1)

    df_duplicados.show()
    println("Hay "+ df_duplicados.count()+" lineas duplicadas")

    // TODO: Manejar nulos en 'genres' reemplazando por "Unknown"
    // Cuando tenga que cambiar valores null que no esten declarados como String, usar funcion na.fill
    // Si no pongo Seq("nombre_columna"), cambiaria todos los null de todas las columnas
    println("- - Nulos en 'genres' reemplazando por Unknown")

    val nulos1 =dfClean.filter(col("genres").isNull).count()
    println("Hay "+nulos1+" valores nulos en la columna genres")

    val dfNulosGenres = dfClean2.na.fill("Unknown", Seq("genres"))
    val nulos = dfNulosGenres.filter(col("genres") === "Unknown").count()
    println("Hay "+nulos+" Unknown en la columna genres")

    val nulos2 = dfNulosGenres.filter(col("genres").isNull).count()
    println("Hay "+nulos2+" valores nulos en la columna genres")

    // Validación Manual:
    // El alumno debe mostrar el esquema final (printSchema()) y 5 filas limpias.
    println("Validación Manual ")
    println("mostrar el esquema final (printSchema()) y 5 filas limpias.")
    dfNulosGenres.show(5)
    dfNulosGenres.printSchema()

    println("Por que usar DecimalType en lugar de DoubleType")
    val a = 0.7
    val b = 0.2
    println(a+b)

    val c = BigDecimal(0.7)
    val d = BigDecimal(0.2)
    println(c+d)

  }
}