package com.github.supermariolabs.spooq

import com.github.supermariolabs.spooq.model.Step
import com.github.supermariolabs.spooq.model.json.StepEncoder
import com.github.supermariolabs.spooq.udf.utils.UdfUtils
import com.mongodb.client.MongoClients
import org.apache.hadoop.shaded.com.fasterxml.jackson.databind.ObjectMapper
import org.apache.sedona.sql.utils.SedonaSQLRegistrator
import org.apache.spark.sql.functions.{col, explode, from_json, map_keys, soundex}
import org.apache.spark.sql.types.{ArrayType, MapType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}
import org.bson.Document
import org.junit.Test

import scala.collection.mutable.ListBuffer


class MyTest {

  @Test
  def test(): Unit = {
    import io.circe._
    //import io.circe.generic.semiauto._
    import io.circe.generic.auto._
    import io.circe.parser._
    import io.circe.syntax._

    implicit val enc: StepEncoder = new StepEncoder

    val step = Step("prova",None,None,None,"sql",None,None,None,None,None,None,None,Some("select * from pippo"),None,None,None,None,None,None,None,None,None,None,None,None)
    println(s"JSON: ${step.asJson}")
    assert(true)
  }

  @Test
  def sedona(): Unit = {
    val spark = SparkSession.builder.appName("Test").master("local[*]").getOrCreate()

    //SedonaSQLRegistrator.registerAll(spark)
    Class.forName("org.apache.sedona.sql.utils.SedonaSQLRegistrator").getDeclaredMethods.foreach( f => println(s"> ${f.toString}") )

    Class.forName("org.apache.sedona.sql.utils.SedonaSQLRegistrator").getMethod("registerAll",Class.forName("org.apache.spark.sql.SparkSession")).invoke(this, spark)

    spark.sql("""
      SELECT
        ST_GeomFromWKT('POINT (2.0 4.0)') as p1,
        ST_GeomFromText('LINESTRING(-71.160281 42.258729,-71.160837 42.259113,-71.161144 42.25932)') as p2
      """).show

  }

  @Test
  def parseJsonColumnToDfWithoutSchema(): Unit = {

    val spark = SparkSession.builder.appName("Test").master("local[*]").getOrCreate()
    import spark.implicits._

    val expectedOutput = Seq(("1","email","office06")).toDF("id","source","office_id")
    expectedOutput.show(false)
    //create fake input df
    val json = """[{"id": "1", "source": "email", "office_id": "office06"}]"""
    val inputDf = Seq(json).toDF("res")
      .withColumn("res", from_json($"res",ArrayType(StringType)))

    val output: DataFrame = UdfUtils.parseJsonColumnToDfWithoutSchema(inputDf, "res")
    output.show(false)
    assert(expectedOutput.schema.equals(output.schema))
    assert(expectedOutput.collect().sameElements(output.collect()))

  }
}
