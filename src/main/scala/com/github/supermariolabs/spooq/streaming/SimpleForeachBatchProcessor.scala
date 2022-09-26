package com.github.supermariolabs.spooq.streaming

import org.apache.spark.sql.DataFrame

trait SimpleForeachBatchProcessor extends Serializable {
  def apply(df: DataFrame, id: Long): Unit
  val processor = apply _
}
