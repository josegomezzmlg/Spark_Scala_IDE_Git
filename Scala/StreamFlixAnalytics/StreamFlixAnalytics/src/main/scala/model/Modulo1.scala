package com.streamflix
package model

import org.apache.spark.{SparkConf, SparkContext}

object Modulo1 {

  def execute(): Unit = {

    val conf = new SparkConf()
      .setAppName("StreamFlix")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")


    val rdd = sc.textFile("src/main/resources/data/server_logs.txt")

    val rdd_filtrado = rdd.filter(linea => linea.startsWith("["))
    val rdd_filtrado_Info = rdd.filter(linea => linea.startsWith("[INFO]"))
    val rdd_filtrado_Error = rdd.filter(linea => linea.startsWith("[ERROR]"))

    // Tarea 1: Filtrar solo las líneas que empiezan con [ERROR] o [INFO]
    println("Tarea 1")
    val filtrado = rdd_filtrado.filter(linea =>linea.startsWith("[INFO]") || linea.startsWith("[ERROR]"))

    filtrado.take(20).foreach(println)
    //rdd_filtrado_Error.take(20).foreach(println)

    // Tarea 2: Mapear para extraer (Nivel, Mensaje)
    println("Tarea 2")
    val nivelMensaje = rdd_filtrado_Error.map(linea => {
      val nivel = linea.split(" ")(0)
      val mensaje = linea.split("\\|").last
      (nivel,mensaje)
    })
    nivelMensaje.take(20).foreach(println)

    // Tarea 3: Contar cuántos errores de tipo 503 ocurrieron usando RDD actions (count, filter)
    println("Tarea 3")
    val er_503 = rdd_filtrado_Error.filter(_.contains("503"))
    println("Hay "+er_503.count()+" errores 503")

    // Tarea 4: Calcular el porcentaje de errores respecto al total de logs válidos

    val errores = rdd_filtrado_Error.count()
    val validos = rdd_filtrado_Info.count()
    val porcentajes = (errores.toDouble/validos.toDouble)*100
    println("De todos los logs, hay "+validos+" validos y "+errores+" erroneos")
    println(f"El porcentaje de error frente al de aciertos es de $porcentajes%.2f"+"%")

    // Validación Manual:
    // El alumno debe imprimir en consola el número exacto de líneas descartadas (corruptas).
    val rddCorrupto = rdd.filter(linea => !linea.startsWith("["))
    val lienasCorruptas = rddCorrupto.count()
    println(s"Hay $lienasCorruptas filas corruptas")

    // Debe generar un archivo de texto simple output/error_counts que contenga pares
    //(Código, Cantidad).

    val errorClaveValor1 = rdd_filtrado_Error.map(linea => linea.split("\\|")(2))

    val errorClaveValor = errorClaveValor1.map(linea => linea.split(":")(1))

    val paresClaveValor = errorClaveValor.map(word => (word,1))

    val errorCounts = paresClaveValor.reduceByKey((a,b)=>a+b)
    // Si no pongo coalesce(1), me genera varios archivos
    errorCounts.coalesce(1).saveAsTextFile("src/main/resources/output/error_counts")

  }
}
