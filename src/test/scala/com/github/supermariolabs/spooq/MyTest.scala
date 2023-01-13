package com.github.supermariolabs.spooq

import com.github.supermariolabs.spooq.model.Step
import com.github.supermariolabs.spooq.model.json.StepEncoder
import com.github.supermariolabs.spooq.udf.utils.UdfUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.types.{ArrayType, StringType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.junit.Test
import java.net.URI
import scala.io.{BufferedSource, Source}


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

  @Test
  def loadConfThroughHadoopLibrary(): Unit = {

    val cfgFile: String = "src/test/resources/testSpooqConf.conf"
    val fs: FileSystem = FileSystem.get(new URI(cfgFile), new Configuration())
    val inputStream: FSDataInputStream = fs.open(new Path(cfgFile))
    val inputResult : String = Source.fromInputStream(inputStream).takeWhile(_ != null).mkString
    val outputStream: BufferedSource = Source.fromFile(cfgFile)
    val expectedOutput: String = outputStream.mkString
    outputStream.close()
    assert(inputResult.equals(expectedOutput))



  }
}
