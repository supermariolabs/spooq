package com.github.supermariolabs.spooq.streaming.hbase

import com.github.supermariolabs.spooq.streaming.SimpleForeachBatchProcessor
import org.apache.spark.sql.DataFrame
import org.slf4j.LoggerFactory

class HBaseForeachBatchProcessor(options: Map[String,String]=Map.empty[String,String]) extends SimpleForeachBatchProcessor{
  val logger = LoggerFactory.getLogger(this.getClass)

  val rowProcessor = new HBaseForeachProcessor(options)

  override def apply(df: DataFrame, id: Long): Unit = {
    logger.info(s"HBaseForeachBatchProcessor::process(batchId: $id)")
    df.foreach(row => {
      rowProcessor.processor.process(row)
      })
  }
}
