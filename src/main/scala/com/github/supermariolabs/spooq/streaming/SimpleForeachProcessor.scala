package com.github.supermariolabs.spooq.streaming

import org.apache.spark.sql.{ForeachWriter, Row}

trait SimpleForeachProcessor extends Serializable {
  val processor: ForeachWriter[Row]
}
