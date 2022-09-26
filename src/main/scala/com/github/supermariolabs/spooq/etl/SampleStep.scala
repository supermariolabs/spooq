package com.github.supermariolabs.spooq.etl

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

class SampleStep extends SimpleStep {
  val logger = LoggerFactory.getLogger(this.getClass)

  override def run(dfMap: Map[String, DataFrame], varMap: Map[String, Any]): DataFrame = {
    val spark = SparkSession.getActiveSession.get

    dfMap.foreach(dfEntry => {
      logger.info(s"DataFrame: ${dfEntry._1} schema:\n${dfEntry._2.schema.toDDL}")
    })

    varMap.foreach(mapEntry => {
      logger.info(s"Variable: ${mapEntry._1} type:\n${mapEntry._2.getClass.getSimpleName}")
    })

    spark.emptyDataFrame
  }
}
