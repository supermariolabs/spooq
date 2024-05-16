package com.github.supermariolabs.spooq.etl

import com.github.supermariolabs.spooq.model.Step
import org.apache.spark.sql.DataFrame

trait CustomInputStep extends Serializable {
  def run(dfMap: Map[String, DataFrame], variables: Map[String, Any], args : Map[String,String], customInputStep : Step): DataFrame
}
