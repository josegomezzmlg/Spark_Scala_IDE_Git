package com.streamflix

import org.apache.spark
import org.apache.spark.sql.catalyst.dsl.expressions.longToLiteral
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object Modulo1 {

  def main(args: Array[String]): Unit = {



    val conf = new SparkConf()
      .setAppName("StreamFlix")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)


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
    val nivel_mensaje = rdd_filtrado_Error.map(linea => {
      val nivel = linea.split(" ")(0)
      val mensaje = linea.split("\\|").last
      (nivel,mensaje)
    })
    nivel_mensaje.take(20).foreach(println)

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
    val rdd_corrupto = rdd.filter(linea => !linea.startsWith("["))
    val lienas_corruptas = rdd_corrupto.count()
    println(s"Hay $lienas_corruptas filas corruptas")

    // Debe generar un archivo de texto simple output/error_counts que contenga pares
    //(Código, Cantidad).

    val error_clave_valor_1 = rdd_filtrado_Error.map(linea => linea.split("\\|")(2))

    val error_clave_valor = error_clave_valor_1.map(linea => linea.split(":")(1))

    val pares_clave_valor = error_clave_valor.map(word => (word,1))

    val error_counts = pares_clave_valor.reduceByKey((a,b)=>a+b)
    // Si no pongo coalesce(1), me genera varios archivos
    error_counts.coalesce(1).saveAsTextFile("src/main/resources/output/error_counts")

  }
}
