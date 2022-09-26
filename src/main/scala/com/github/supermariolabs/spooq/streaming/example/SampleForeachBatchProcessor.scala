package com.github.supermariolabs.spooq.streaming.example

import com.github.supermariolabs.spooq.streaming.SimpleForeachBatchProcessor
import org.apache.spark.sql.DataFrame
import org.slf4j.LoggerFactory

class SampleForeachBatchProcessor(options: Map[String,String]=Map.empty[String,String]) extends SimpleForeachBatchProcessor{
  val logger = LoggerFactory.getLogger(this.getClass)

  options.foreach(opt => {
    logger.info(s"${opt._1} -> ${opt._2}")
  })

  override def apply(df: DataFrame, id: Long): Unit = {
    logger.info(s"SampleForeachBatchProcessor::process(batchId: $id)")
    df.foreach(row => {
      var rowStr = ""
      row.schema.foreach(field => {
        rowStr+=(s", ${field.name} [${field.dataType.sql}] -> ${row.get(row.fieldIndex(field.name))}")
      })
      logger.info("Row: "+rowStr.substring(2))
    })
  }
}
