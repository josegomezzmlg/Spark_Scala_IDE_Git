package com.streamflix
package model

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import functions.functions.limpiarInfo

import .loadData

object Modulo3 {
  def execute(spark: SparkSession): Unit = {
    spark.sparkContext.setLogLevel("ERROR")

    val pathLog = "src/main/resources/data/server_logs.txt"
    val pathMovies = "src/main/resources/data/movies_metadata.csv"
    val (dfServerlogs, dfMovies) = loadData(spark, pathLog, pathMovies)

    val df1 = tableSorting(joinLogsMovies(cleanLogs(dfServerlogs),cleanMovies(dfMovies)))

    df1.select("*").limit(10).show()

  }
    def loadData(spark: SparkSession, pathLog: String, pathMovies: String): (DataFrame, DataFrame) = {

      val dfServerlogs = spark.read.text(pathLog)

      val dfMovies = spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(pathMovies)

      (dfServerlogs, dfMovies)
    }

  // TODO: Limpieza del DF server_logs.txt

    def cleanLogs(dfServerlogs:DataFrame):DataFrame= {
      val infoDF = dfServerlogs.filter(col("value").startsWith("[INFO]"))

      // Uso de la funcion LimpiarInfo, usada en los modulos 3,4 y 5
      val dfInfoSeparado = limpiarInfo(infoDF).drop(col("value")).drop(col("nivel"))

      val dfInfoSeparado2 = dfInfoSeparado.withColumn("userId", split(col("userId"), ":")(1))
        .withColumn("movieId", split(col("movieId"), ":")(1))
        .withColumn("durationWatched", split(col("durationWatched"), ":")(1))

      val logsDF = dfInfoSeparado2.withColumn("userId",col("userId").cast("int"))
        .withColumn("durationWatched",col("durationWatched").cast("int"))
        .withColumn("movieId",split(col("movieId"), "_")(1))

      logsDF
    }

  // TODO: limpieza del DF movies_metadatas

  def cleanMovies(dfMovies:DataFrame):DataFrame= {

    val moviesDF = dfMovies.withColumn("id",col("id").cast("int"))
      .withColumn("genres",regexp_replace(col("genres"),"\\|",","))
      .drop(col("subscription_price")).drop(col("release_date")).drop(col("country"))

    moviesDF

  }

  def joinLogsMovies(logsDF:DataFrame,moviesDF:DataFrame):DataFrame= {

    val dfSm = logsDF.join(broadcast(moviesDF),
      logsDF("movieId")===moviesDF("id"),"inner")
    val dfSM = dfSm.select("userId","title","genres","durationWatched","movieId","id").sort("id")

    dfSM
  }

  def tableSorting(dfSM:DataFrame):DataFrame= {
    val genreMetricsDF = dfSM
      .withColumn("genre", explode(split(col("genres"), "\\|")))
      .groupBy("genre")
      .agg(sum("durationWatched").alias("total_hours"))
      .sort("total_hours")

    genreMetricsDF
  }
}
