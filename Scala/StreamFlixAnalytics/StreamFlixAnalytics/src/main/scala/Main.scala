package com.streamflix

import model.{Modulo1, Modulo2, Modulo3, Modulo4, Modulo5}
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext

object Main {

  def main(args: Array[String]): Unit = {

    if (args.isEmpty) {
      System.exit(1)
    }

    val modulo = args(0)

    implicit val spark: SparkSession = SparkSession.builder()
      .appName("StreamFlixAnalytics")
      .master("local[*]")
      .getOrCreate()

    implicit val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")

    modulo match {
      case "1" =>
        println("Ejecutando Módulo 1...")
        Modulo1.execute()

      case "2" =>
        println("Ejecutando Módulo 2...")
        Modulo2.execute(spark)

      case "3" =>
        println("Ejecutando Módulo 3...")
        Modulo3.execute(spark)

      case "4" =>
        println("Ejecutando Módulo 4...")
        Modulo4.execute(spark)

      case "5" =>
        println("Ejecutando Módulo 5...")
        Modulo5.execute(spark)

      case _ =>
        println(s"El módulo $modulo no existe todavía.")
        System.exit(1)
    }
    spark.stop()
  }
}