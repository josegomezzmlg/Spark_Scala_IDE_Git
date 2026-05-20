package com.streamflix
package model

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{broadcast, col, regexp_replace, split, substring}
import functions.functions.limpiarInfo

object Modulo5 {
  def execute(spark: SparkSession): Unit = {

    import spark.implicits._
    spark.sparkContext.setLogLevel("ERROR")

    val dfServerlogs = spark.read
      .text("src/main/resources/data/server_logs.txt")

    val dfMovies = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("src/main/resources/data/movies_metadata.csv")

    val infoDF = dfServerlogs.filter(col("value").startsWith("[INFO]"))

    // Uso de la funcion LimpiarInfo, usada en los modulos 3,4 y 5

    val dfInfoSeparado =  limpiarInfo(infoDF).drop(col("nivel"))

    val dfInfoSeparado2 = dfInfoSeparado.withColumn("userId", split(col("userId"), ":")(1))
      .withColumn("movieId", split(col("movieId"), ":")(1))
      .withColumn("durationWatched", split(col("durationWatched"), ":")(1))

    val logsDF = dfInfoSeparado2.withColumn("userId",col("userId").cast("int"))
      .withColumn("durationWatched",col("durationWatched").cast("int"))
      .withColumn("movieId",split(col("movieId"), "_")(1))
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
      logsDF("movieId")===moviesDF("id"),"inner")

    val df_1 = dfSm.select("movieId","id","userId","timestamp","durationWatched","title","genres","subscription_price","release_date","country")
      .sort("release_date","country")

    df_1.show()

    //df_1.write.mode("overwrite")
    //  .partitionBy("release_date", "country")
    //  .parquet("src/main/resources/output/analytics_warehouse")




  }
}
