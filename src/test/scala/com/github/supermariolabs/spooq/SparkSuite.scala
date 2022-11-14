package com.github.supermariolabs.spooq

import org.apache.spark.sql.SparkSession
import org.scalatest.Suite

import scala.util.Try

trait SparkSuite extends Suite {
  lazy val sparkSession: SparkSession = {
    SparkSuite.spark.newSession()
  }
}

object SparkSuite {
  private val warehouseLocation = "file:$" + "{system:user.dir}/spark-warehouse"
  private lazy val spark = {
    val ss = SparkSession
      .builder()
      .appName("test suite")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .config("spark.master", "local[1,4]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "1")
      .config("spark.task.maxFailures", "2")
      .config("spark.sql.session.timeZone", "UTC")
      .getOrCreate()

    ss.sparkContext.setLogLevel("OFF")

    sys.addShutdownHook {
      Try {
        ss.close()
      }
    }
    ss
  }
}
