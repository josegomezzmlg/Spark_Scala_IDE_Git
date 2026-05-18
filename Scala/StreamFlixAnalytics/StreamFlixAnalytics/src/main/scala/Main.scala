package com.streamflix

import model.{Modulo1, Modulo2, Modulo3, Modulo4, Modulo5}


object Main {

  def main(args: Array[String]): Unit = {

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
        Modulo1.ejecutar()

      case 2 =>
        println("Ejecutando Módulo 2...")
        Modulo2.ejecutar()

      case 3 =>
        println("Ejecutando Módulo 3...")
        Modulo3.ejecutar()

      case 4 =>
        println("Ejecutando Módulo 4...")
        Modulo4.ejecutar()

      case 5 =>
        println("Ejecutando Módulo 5...")
        Modulo5.ejecutar()

      case _ =>
        println("Opción no válida")

    }

  }

}
