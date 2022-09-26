package com.github.supermariolabs.spooq.udf.avro

import com.github.supermariolabs.spooq.udf.SimpleUDF
import org.apache.avro.Schema
import org.apache.spark.sql.avro.SchemaConverters
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions
import org.apache.spark.sql.types.StructType

class SchemaToDDL extends SimpleUDF {

  def convert(schemaStr: String): String = {
    SchemaConverters.toSqlType(
      new Schema.Parser().parse(schemaStr)
    ).dataType.asInstanceOf[StructType].toDDL
  }

  override val udf: UserDefinedFunction = functions.udf(convert(_:String))
}
