package com.github.supermariolabs.spooq.udf.example

import com.github.supermariolabs.spooq.udf.SimpleUDF
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions

class SampleUDF extends SimpleUDF {

  def sayHello(name: String): String = {
    return s"Hello, $name"
  }

  override val udf: UserDefinedFunction = functions.udf(sayHello(_: String))
}
