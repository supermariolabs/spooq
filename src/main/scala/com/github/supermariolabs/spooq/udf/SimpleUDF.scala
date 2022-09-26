package com.github.supermariolabs.spooq.udf

import org.apache.spark.sql.expressions.UserDefinedFunction

trait SimpleUDF extends Serializable {
    val udf: UserDefinedFunction
}
