package com.github.supermariolabs.spooq.api

import com.github.supermariolabs.spooq.Engine
import com.github.supermariolabs.spooq.model.json.ProcessingOutputEncoder
import com.github.supermariolabs.spooq.model.{ProcessingOutput, Report, Step}
import org.slf4j.LoggerFactory
import spark.{Request, Response, Spark}
import io.circe._
//import io.circe.generic.semiauto._
import io.circe.generic.auto._
import io.circe.parser._
import io.circe.syntax._
import org.apache.spark.sql.SparkSession

import scala.io.Source

class HttpServer(engine: Engine)(implicit spark: SparkSession) {
  val logger = LoggerFactory.getLogger(this.getClass)

  def run(): Unit = {
    val port = engine.conf.httpPort.getOrElse("4242").toInt
    logger.info(s"[Experimental] Starting HTTP Server (port: $port)...")
    Spark.port(port)

    //CORS enabled?
    if (engine.conf.httpCors.getOrElse(false)) {
      Spark.options("/*", (req: Request, res: Response) => {
          val accessControlRequestHeaders = req.headers("Access-Control-Request-Headers")
          if (accessControlRequestHeaders != null) res.header("Access-Control-Allow-Headers", accessControlRequestHeaders)
          val accessControlRequestMethod = req.headers("Access-Control-Request-Method")
          if (accessControlRequestMethod != null) res.header("Access-Control-Allow-Methods", accessControlRequestMethod)

        "OK"
        })

      Spark.before((request, response) => {
        response.header("Access-Control-Allow-Origin", "*")
        response.header("Access-Control-Allow-Headers", "*")
      })
    }
    Spark.get("/", (req: Request, res: Response) => {
      val banner = Source.fromInputStream(getClass.getClassLoader.getResourceAsStream("banner.txt")).mkString
      val build = Source.fromInputStream(getClass.getClassLoader.getResourceAsStream("lastbuild")).mkString

      s"${banner.replace("$$BUILD$$", build)}"
    })

    Spark.post("/sql", (req: Request, res: Response) => {
      //implicit val sqlRequestDecoder: Decoder[SqlRequest] = deriveDecoder[SqlRequest]
      val decodedSql = decode[SqlRequest](req.body)
      var out: Option[Array[String]] = None
      var id = "-1"

      decodedSql match {
        case Right(s) => {
          id = s.id
          out = Some(spark.sql(s.sql).toJSON.collect)
        }
        case _ =>
      }

      res.`type`("application/json")

      var retVal = "Empty"

      out match {
        case Some(notEmpty) => retVal = s"""{"id":"$id", "res":[${out.get.mkString(",")}]}"""
        case None =>
      }

      retVal
    })

    Spark.post("/step", (req: Request, res: Response) => {
      //implicit val stepDecoder: Decoder[Step] = deriveDecoder[Step]
      val decodedStep = decode[Step](req.body())
      var exitStatus: Option[Report] = None

      decodedStep match {
        case Right(s) => {
          exitStatus = engine.parseStep(s)
        }
        case Left(e) => e.printStackTrace
      }

      res.`type`("application/json")
      //implicit val outEncoder: Encoder[ProcessingOutput] = deriveEncoder[ProcessingOutput]
      implicit val encodePo: Encoder[ProcessingOutput] = new ProcessingOutputEncoder
      s"""${exitStatus.get.done.asJson}"""
    })
  }
}
