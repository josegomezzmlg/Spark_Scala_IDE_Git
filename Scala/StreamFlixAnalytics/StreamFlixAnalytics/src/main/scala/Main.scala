package com.streamflix

import model.{Modulo1, Modulo2, Modulo3, Modulo4, Modulo5, Modulo5_1}

import org.apache.spark.sql.SparkSession


object Main {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("StreamFlixAnalytics")
      .master("local[*]")
      .getOrCreate()

    println("STREAMFLIX ANALYTICS")
    println("===================================")
    println("1 -> Ejecutar Módulo 1")
    println("2 -> Ejecutar Módulo 2")
    println("3 -> Ejecutar Módulo 3")
    println("4 -> Ejecutar Módulo 4")
    println("5 -> Ejecutar Módulo 5")
    println("===================================")

    print("Selecciona una opción: ")

    val opcion = scala.io.StdIn.readInt()

    opcion match {

      case 1 =>
        println("Ejecutando Módulo 1...")
        Modulo1.execute()

      case 2 =>
        println("Ejecutando Módulo 2...")
        Modulo2.execute(spark)

      case 3 =>
        println("Ejecutando Módulo 3...")
        Modulo3.execute(spark)

      case 4 =>
        println("Ejecutando Módulo 4...")
        Modulo4.execute(spark)

      case 5 =>
        println("Ejecutando Módulo 5...")
        Modulo5.execute(spark)

      case 6 =>
        println("Ejecutando Módulo 5_1...")
        Modulo5_1.execute(spark)

      case _ =>
        println("Opción no válida")

    }

  }

}
