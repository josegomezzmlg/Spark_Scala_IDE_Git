package com.streamflix
package model

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{broadcast, col, regexp_replace, split, substring}

object Modulo5 {
  def ejecutar(): Unit = {
    val spark = SparkSession.builder()
      .appName("StreamFlix")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val dfServerlogs = spark.read
      .text("src/main/resources/data/server_logs.txt")

    val dfMovies = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("src/main/resources/data/movies_metadata.csv")

    val dfInfo = dfServerlogs.filter(col("value").startsWith("[INFO]"))

    val dfInfoSeparado = dfInfo.withColumn("No usar",split(col("value"),"\\|")(0))
      .withColumn("user_id",split(col("value"),"\\|")(1))
      .withColumn("movie_id",split(col("value"),"\\|")(2))
      .withColumn("duration_watched",split(col("value"),"\\|")(3)).drop(col("No usar"))

    val dfInfoSeparado2 = dfInfoSeparado.withColumn("user_id", split(col("user_id"), ":")(1))
      .withColumn("movie_id", split(col("movie_id"), ":")(1))
      .withColumn("duration_watched", split(col("duration_watched"), ":")(1))

    val logsDF = dfInfoSeparado2.withColumn("user_id",col("user_id").cast("int"))
      .withColumn("duration_watched",col("duration_watched").cast("int"))
      .withColumn("movie_id",split(col("movie_id"), "_")(1))
      .withColumn("value", substring(col("value"), 8, 19))
      .withColumnRenamed("value", "timestamp")

    // Apartado movies_metadata

    val moviesDF = dfMovies.withColumn("id",col("id").cast("int"))
      .withColumn("genres",regexp_replace(col("genres"),"\\|",","))
      .withColumn("subscription_price",regexp_replace(col("subscription_price"),"\\$",""))
      .withColumn("release_date",regexp_replace(col("release_date"),"invalid-date","ERROR"))
      .withColumn("release_date", split(col("release_date"),"-")(0))

    logsDF.show()
    logsDF.printSchema()
    moviesDF.show()

    val dfSm = logsDF.join(broadcast(moviesDF),
      logsDF("movie_id")===moviesDF("id"),"inner")

    val df_1 = dfSm.select("movie_id","id","user_id","timestamp","duration_watched","title","genres","subscription_price","release_date","country")
      .sort("release_date","country")

    df_1.show()

    df_1.write.mode("overwrite")
      .partitionBy("release_date", "country")
      .parquet("src/main/resources/output/analytics_warehouse")




  }
}
