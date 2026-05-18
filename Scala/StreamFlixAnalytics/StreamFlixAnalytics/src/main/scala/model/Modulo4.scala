package com.streamflix
package model

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._


object Modulo4 {

  def ejecutar(): Unit = {

    val spark = SparkSession.builder()
      .appName("StreamFlix")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val rawDF = spark.read.text("src/main/resources/data/server_logs.txt")

    val infoDF = rawDF.filter(col("value").startsWith("[INFO]"))

    val dfInfoSeparado = infoDF.withColumn("nivel", split(col("value"), "\\|")(0))
      .withColumn("userId", split(col("value"), "\\|")(1))
      .withColumn("movieId", split(col("value"), "\\|")(2))
      .withColumn("durationWatched", split(col("value"), "\\|")(3))
      .drop(col("value"))

    val dfInfoSeparado2 = dfInfoSeparado
      .withColumn("timestampStr",concat_ws(" ", split(col("nivel"), " ")(1), split(col("nivel"), " ")(2)))
      .withColumn("userId", split(col("userId"), ":")(1))
      .withColumn("movieId", split(col("movieId"), ":")(1))
      .withColumn("durationWatched", split(col("durationWatched"), ":")(1))
      .drop(col("nivel"))

    val logsDF = dfInfoSeparado2
      .withColumn("userId", col("userId").cast("int"))
      .withColumn("durationWatched", col("durationWatched").cast("int"))
      .withColumn("movieId", split(col("movieId"), "_")(1))
      .withColumn("timestamp",to_timestamp(col("timestampStr"), "yyyy-MM-dd HH:mm:ss"))
      .drop(col("timestampStr"))
      .filter(col("timestamp").isNotNull)
      .filter(col("durationWatched").isNotNull)

    logsDF.show(false)

    // Binge = Maraton

    val windowSpec = Window.partitionBy("userId").orderBy("timestamp")

    val lagDF = logsDF
      .withColumn("prevTimestamp", lag("timestamp", 1).over(windowSpec))
      .withColumn("prevDurationWatched", lag("durationWatched", 1).over(windowSpec))

    val bingeDF = lagDF.withColumn(
        "pauseMinutes",(unix_timestamp(col("timestamp")) - unix_timestamp(col("prevTimestamp"))
          ) / 60 - col("prevDurationWatched")
      ).withColumn("isBinge",when(col("pauseMinutes") >= 0 && col("pauseMinutes") < 20, 1).otherwise(0))

    println("Diferencia en minutos entre timestamp y prev_timestamp")
    println("Creacion de la columna \"is_binge\" si la diferencia < 20 mins")
    bingeDF.show()

    val sessionWindow = Window.partitionBy("userId").orderBy("timestamp")

    val sessionDF = bingeDF.withColumn("newSession",
        when(col("isBinge") === 1, 0).otherwise(1))
      .withColumn("sessionId",sum(col("newSession")).over(sessionWindow))

    val bingeDF2 = sessionDF.groupBy("userId", "sessionId")
      .agg(
        count("*").alias("itemsWatched"),
        min("timestamp").alias("sessionStart"),
        max("timestamp").alias("sessionEnd"),
        sum("durationWatched").alias("totalWatchMinutes")
      ).filter(col("itemsWatched") > 3)

    bingeDF2.show()

    println("Top 10 Binge Watchers")
    val top10BingeWatchersDF = bingeDF2
      .groupBy("userId")
      .agg(
        count("*").alias("bingeSessions"),
        sum("itemsWatched").alias("totalItemsWatched"),
        sum("totalWatchMinutes").alias("totalWatchMinutes")
      ).orderBy(desc("bingeSessions"),desc("totalItemsWatched")).limit(10)

    top10BingeWatchersDF.show(false)

    println("Top 3 usuarios más adictos ")
    val top3AdictosDF = top10BingeWatchersDF
      .orderBy(desc("totalWatchMinutes")).limit(3)

    top3AdictosDF.show(false)

  }
}