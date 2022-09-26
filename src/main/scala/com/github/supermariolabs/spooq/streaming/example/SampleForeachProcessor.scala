package com.github.supermariolabs.spooq.streaming.example

import com.github.supermariolabs.spooq.streaming.SimpleForeachProcessor
import org.apache.spark.sql.{ForeachWriter, Row}
import org.slf4j.LoggerFactory

class SampleForeachProcessor(options: Map[String,String]=Map.empty[String,String]) extends SimpleForeachProcessor {
  val logger = LoggerFactory.getLogger(this.getClass)

  logger.info("SampleForeachProcessor init")
  options.foreach(opt => {
    logger.info(s"${opt._1} -> ${opt._2}")
  })

  override val processor: ForeachWriter[Row] = new ForeachWriter[Row] {
    override def open(partitionId: Long, epochId: Long): Boolean = {
      //logger.info(s"SampleForeachProcessor::open($partitionId, $epochId)")
      true
    }

    override def process(row: Row): Unit = {
      //logger.info(s"SampleForeachProcessor::process($row)")
      var rowStr = ""
      row.schema.foreach(field => {
        rowStr+=(s", ${field.name} [${field.dataType.sql}] -> ${row.get(row.fieldIndex(field.name))}")
      })
      logger.info("Row: "+rowStr.substring(2))
    }

    override def close(errorOrNull: Throwable): Unit = {
      if (errorOrNull!=null) logger.error(s"SampleForeachProcessor::close(${errorOrNull.getMessage})")
    }
  }
}
