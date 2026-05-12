package com.streamflix

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Modulo3 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("StreamFlix")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val df_serverlogs = spark.read
      .text("src/main/resources/data/server_logs.txt")
    val df_movies = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("src/main/resources/data/movies_metadata.csv")

    // TODO: Limpieza del DF server_logs.txt
    val df_Info = df_serverlogs.filter(col("value").startsWith("[INFO]"))

    val df_Info_separado = df_Info.withColumn("No usar",split(col("value"),"\\|")(0))
      .withColumn("user_id",split(col("value"),"\\|")(1))
      .withColumn("movie_id",split(col("value"),"\\|")(2))
      .withColumn("duration_watched",split(col("value"),"\\|")(3)).drop(col("value")).drop(col("No usar"))

    val df_Info_separado_2 = df_Info_separado.withColumn("user_id", split(col("user_id"), ":")(1))
      .withColumn("movie_id", split(col("movie_id"), ":")(1))
      .withColumn("duration_watched", split(col("duration_watched"), ":")(1))

    val logsDF = df_Info_separado_2.withColumn("user_id",col("user_id").cast("int"))
      .withColumn("duration_watched",col("duration_watched").cast("int"))
      .withColumn("movie_id",split(col("movie_id"), "_")(1))
    // Este es el df de server_logs con el que voy a trabajar
    logsDF.show()
    println("Lineas de logs "+logsDF.count())

    // TODO: limpieza del DF movies_metadatas
    val moviesDF = df_movies.withColumn("id",col("id").cast("int"))
      .withColumn("genres",regexp_replace(col("genres"),"\\|",","))
      .drop(col("subscription_price")).drop(col("release_date")).drop(col("country"))

    // Este es el df de movies_metadata con el que voy a trabajar
    println("Lineas de peliculas "+moviesDF.count())
    moviesDF.show()

    val df_sm = logsDF.join(broadcast(moviesDF),
      logsDF("movie_id")===moviesDF("id"),"inner")
    val df_sm_1 = df_sm.select("user_id","title","genres","duration_watched","movie_id","id").sort("id")
    df_sm_1.show()
    df_sm_1.explain(true)

    val genreMetricsDF = df_sm_1
      .withColumn("genre", explode(split(col("genres"), "\\|")))
      .groupBy("genre")
      .agg(sum("duration_watched").alias("total_hours"))

    genreMetricsDF.sort("total_hours").show()







    // [INFO] 2025-05-15 11:14:41|User:37998|Play:Movie_1|Dur:117

  }

}
