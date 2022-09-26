package com.github.supermariolabs.spooq.model

import org.apache.spark.sql.DataFrame

case class EngineOut(dataFrames: Map[String, DataFrame], variables: java.util.Map[String, Any], report: Map[String, Report])
