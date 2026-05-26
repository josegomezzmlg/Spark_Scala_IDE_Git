package com.streamflix
package model

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{broadcast, col, regexp_replace, split, substring}
import functions.functions.limpiarInfo

import com.streamflix.variables.variable.{pathLog, pathMovies}

object Modulo5 {
  def execute(spark: SparkSession): Unit = {

    spark.sparkContext.setLogLevel("ERROR")

    val (dfServerlogs, dfMovies) = loadData(spark, pathLog, pathMovies)


    val dfSm = seleccionarResultado(unirLogsMovies(limpiarLogs(dfServerlogs),limpiarMovies(dfMovies)))

    dfSm.show()

    // guardarParquet(df_1)
  }

  def loadData(spark: SparkSession, pathLog: String, pathMovies: String): (DataFrame, DataFrame) = {

    val dfServerlogs = spark.read.text(pathLog)

    val dfMovies = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(pathMovies)

    (dfServerlogs, dfMovies)
  }

  def limpiarLogs(dfServerlogs: DataFrame): DataFrame = {

    val infoDF = dfServerlogs.filter(col("value").startsWith("[INFO]"))

    val dfInfoSeparado = limpiarInfo(infoDF).drop("nivel")

    val dfInfoSeparado2 = dfInfoSeparado
      .withColumn("userId", split(col("userId"), ":")(1))
      .withColumn("movieId", split(col("movieId"), ":")(1))
      .withColumn("durationWatched", split(col("durationWatched"), ":")(1))

    val logsDF = dfInfoSeparado2
      .withColumn("userId", col("userId").cast("int"))
      .withColumn("durationWatched", col("durationWatched").cast("int"))
      .withColumn("movieId", split(col("movieId"), "_")(1))
      .withColumn("value", substring(col("value"), 8, 19))
      .withColumnRenamed("value", "timestamp")

    logsDF
  }

  def limpiarMovies(dfMovies: DataFrame): DataFrame = {

    val moviesDF = dfMovies
      .withColumn("id", col("id").cast("int"))
      .withColumn("genres", regexp_replace(col("genres"), "\\|", ","))
      .withColumn("subscription_price", regexp_replace(col("subscription_price"), "\\$", ""))
      .withColumn("release_date", regexp_replace(col("release_date"), "invalid-date", "ERROR"))
      .withColumn("release_date", split(col("release_date"), "-")(0))

    moviesDF
  }

  def unirLogsMovies(logsDF: DataFrame, moviesDF: DataFrame): DataFrame = {

    logsDF.join(
      broadcast(moviesDF),
      logsDF("movieId") === moviesDF("id"),"inner"
    )
  }

  def seleccionarResultado(dfSm: DataFrame): DataFrame = {

    dfSm.select("movieId","id","userId","timestamp","durationWatched","title","genres","subscription_price","release_date","country")
      .sort("release_date", "country")
  }

  def guardarParquet(df_1: DataFrame): Unit = {

    df_1.write
      .mode("overwrite")
      .partitionBy("release_date", "country")
      .parquet("src/main/resources/output/analytics_warehouse")
  }
}
