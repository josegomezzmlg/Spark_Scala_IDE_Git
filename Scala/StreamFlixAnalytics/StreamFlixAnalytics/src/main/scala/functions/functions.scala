package com.streamflix
package functions
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{col, lit,split, when}
import org.apache.spark.sql.types.{DataType, DecimalType}

object functions {

  def castDecimalNullLiteral(colName : String, precision:Int,scale:Int): Column={
    when(col(colName) === lit("null"),lit(null)).otherwise(col(colName).cast(DecimalType(precision,scale)))
  }
  // TODO: Esta funcion se encarga de hacer una primera limpieza del df del fichero server_logs.txt y estructuracion
  // Divide el df en varias columnas ( nivel,userID,movieId,durationWatched

  def limpiarInfo(infoDF: DataFrame): DataFrame = {
    infoDF
      .withColumn("nivel", split(col("value"), "\\|")(0))
      .withColumn("userId", split(col("value"), "\\|")(1))
      .withColumn("movieId", split(col("value"), "\\|")(2))
      .withColumn("durationWatched", split(col("value"), "\\|")(3))


  }
}