package com.github.supermariolabs.spooq.logging

import org.apache.spark.sql.DataFrame
import java.io.ByteArrayOutputStream


object SparkUtils {

  def dfAsString(df: DataFrame, numRows: Int = 10, truncate: Boolean = false): String = {
    val outCapture = new ByteArrayOutputStream()
    Console.withOut(outCapture) {
      df.show(numRows = numRows, truncate = truncate)
    }

    new String(outCapture.toByteArray)
  }

}
