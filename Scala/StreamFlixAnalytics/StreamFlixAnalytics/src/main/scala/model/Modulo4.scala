package com.streamflix
package model

import com.streamflix.functions.functions.limpiarInfo
import com.streamflix.variables.variable.pathLog
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object Modulo4 {

  def execute(spark: SparkSession): Unit = {

    spark.sparkContext.setLogLevel("ERROR")

    val rawDF = loadData(spark,pathLog)
    val dfDiffence = differenceTimestampPrevTime(CleanLogs(rawDF))
    val dfTop10 = top10(calculationSessions(dfDiffence))
    val df3MoreAddicts = addicts3(dfTop10)

    println("diferencia en minutos entre 'timestamp' y 'prev_timestamp'")
    dfDiffence.show()
    println("Top 10 Binge Watchers")
    dfTop10.show()
    println("3 usuarios más adictos.")
    df3MoreAddicts.show()
  }

  def loadData(spark: SparkSession, pathLog: String): (DataFrame) = {

      val rawDF = spark.read.text(pathLog)

      rawDF
    }

  def CleanLogs(rawDF: DataFrame): DataFrame= {

    val infoDF = rawDF.filter(col("value").startsWith("[INFO]"))

    // Uso de la funcion LimpiarInfo, usada en los modulos 3,4 y 5

    val dfInfoSeparado = limpiarInfo(infoDF).drop(col("value"))

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

    logsDF

  }

  def differenceTimestampPrevTime(logsDF:DataFrame):DataFrame = {

    val windowSpec = Window.partitionBy("userId").orderBy("timestamp")

    val lagDF = logsDF
      .withColumn("prevTimestamp", lag("timestamp", 1).over(windowSpec))
      .withColumn("prevDurationWatched", lag("durationWatched", 1).over(windowSpec))

    val bingeDF = lagDF.withColumn(
      "pauseMinutes",(unix_timestamp(col("timestamp")) - unix_timestamp(col("prevTimestamp"))
        ) / 60 - col("prevDurationWatched")
    ).withColumn("isBinge",when(col("pauseMinutes") >= 0 && col("pauseMinutes") < 20, 1).otherwise(0))

    bingeDF

  }

  def calculationSessions(bingeDF:DataFrame):DataFrame= {
    val windowSpec = Window.partitionBy("userId").orderBy("timestamp")

    val sessionDF = bingeDF.withColumn("newSession",
        when(col("isBinge") === 1, 0).otherwise(1))
      .withColumn("sessionId",sum(col("newSession")).over(windowSpec))

    val bingeDF2 = sessionDF.groupBy("userId", "sessionId")
      .agg(
        count("*").alias("itemsWatched"),
        min("timestamp").alias("sessionStart"),
        max("timestamp").alias("sessionEnd"),
        sum("durationWatched").alias("totalWatchMinutes")
      ).filter(col("itemsWatched") > 3)

    bingeDF2

  }

  def top10(bingeDF2:DataFrame):DataFrame= {

    val top10BingeWatchersDF = bingeDF2
      .groupBy("userId")
      .agg(
        count("*").alias("bingeSessions"),
        sum("itemsWatched").alias("totalItemsWatched"),
        sum("totalWatchMinutes").alias("totalWatchMinutes")
      ).orderBy(desc("bingeSessions"),desc("totalItemsWatched")).limit(10)

    top10BingeWatchersDF
  }

  def addicts3(top10BingeWatchersDF:DataFrame):DataFrame= {
    val top3AdictosDF = top10BingeWatchersDF
      .orderBy(desc("totalWatchMinutes")).limit(3)

    top3AdictosDF
  }
}