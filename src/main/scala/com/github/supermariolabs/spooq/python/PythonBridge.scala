package com.github.supermariolabs.spooq.python

import org.apache.spark.deploy.PythonRunner
import org.apache.spark.sql.SparkSession

object PythonBridge {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("RunPythonExample")
      .getOrCreate
    spark.range(10).createOrReplaceTempView("pippo")
    PythonRunner.main(("py/hello.py"::""::Nil).toArray)
    spark.stop
  }

}
