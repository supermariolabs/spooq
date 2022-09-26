package org.apache.spark.sql.avro

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.lit

object functions {

  def from_avro(data: Column, jsonFormatSchema: String): Column = {
    return lit("ERROR 'from_avro' function is supported on Spark 3 and later!")
  }

}
