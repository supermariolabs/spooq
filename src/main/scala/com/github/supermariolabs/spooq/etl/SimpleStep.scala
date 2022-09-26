package com.github.supermariolabs.spooq.etl

import org.apache.spark.sql.DataFrame

trait SimpleStep extends Serializable {
  def run(dfMap: Map[String, DataFrame], variables: Map[String, Any]): DataFrame
}
