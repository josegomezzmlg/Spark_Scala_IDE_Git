package com.streamflix
package functions
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, lit, when}
import org.apache.spark.sql.types.{DecimalType, DataType}

class functions {
  def castDecimalNullLiteral(colName : String, precision:Int,scale:Int): Column={
    when(col(colName) === lit("null"),lit(null)).otherwise(col(colName).cast(DecimalType(precision,scale)))
  }

}
