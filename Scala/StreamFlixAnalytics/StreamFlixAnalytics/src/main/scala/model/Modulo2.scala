package com.streamflix
package model

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

object Modulo2 {
  def execute(spark: SparkSession): Unit = {
    // Si no pongo el SetLogLevel me aparecen todas las lineas de INFO
    spark.sparkContext.setLogLevel("ERROR")
    val pathMovies = "src/main/resources/data/movies_metadata.csv"
    val dfMovies = loadData(spark,pathMovies)
    val (dfNulos, dfDuplicados) = analisisNullsDuplicate(cleanMovie(dfMovies))
    val dfManejo = reemplazo(cleanMovie(dfMovies))

    println(" Analisis de Nulos y Duplicados")
    println("Nulos")
    dfNulos.show()
    println("Duplicados")
    dfDuplicados.show()
    println("Reemplazos")
    dfManejo.show()


  }


  def loadData(spark: SparkSession, pathMovies: String): ( DataFrame) = {

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
      .csv(pathMovies)

    dfMovies

  }
    //Tarea:
    //  Cargar un CSV de catálogo de películas (movies_metadata.csv). El archivo tiene precios en
    //formato string con símbolos de moneda (ej: "$12.99") y géneros separados por pipes |. Debes
    //limpiarlo y tiparlo correctamente.
    //Dataset (movies_metadata.csv):



    // TODO: Crear una UDF o usar expresiones select para limpiar el precio
    //  (quitar '$' y castear a Double)

  def cleanMovie(dfMovies:DataFrame):DataFrame= {

  val dfClean = dfMovies.withColumn("subscription_price",
    regexp_replace(col("subscription_price"),"\\$","").cast("decimal(10,2)"))
      .withColumn("genres",regexp_replace(col("genres"),"\\|",","))

    dfClean

  }

  def analisisNullsDuplicate(dfClean:DataFrame):(DataFrame,DataFrame)= {
    // TODO: Hacer un análisis de nulos y duplicados en las columnas

    val dfNulos = dfClean.filter(
      dfClean.columns
        .map(c => col(c).isNull)
        .reduce((cond1, cond2) => cond1 || cond2)
    )

    val dfDuplicados = dfClean.groupBy(dfClean.columns.map(col): _*)
      .count()
      .filter(col("count") > 1)

    (dfNulos,dfDuplicados)
  }

  // TODO: Manejar nulos en 'genres' reemplazando por "Unknown"
  // Cuando tenga que cambiar valores null que no esten declarados como String, usar funcion na.fill
  // Si no pongo Seq("nombre_columna"), cambiaria todos los null de todas las columnas

  def reemplazo(dfClean:DataFrame):DataFrame= {

    val nulos1 =dfClean.filter(col("genres").isNull).count()
    println("Hay "+nulos1+" valores nulos en la columna genres")

    val dfNulosGenres = dfClean.na.fill("Unknown", Seq("genres"))
    val nulos = dfNulosGenres.filter(col("genres") === "Unknown").count()
    println("Hay "+nulos+" Unknown en la columna genres")

    val nulos2 = dfNulosGenres.filter(col("genres").isNull).count()
    println("Hay "+nulos2+" valores nulos en la columna genres")

    dfNulosGenres


  }


}