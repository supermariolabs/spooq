package com.github.supermariolabs.spooq.api

import com.github.supermariolabs.spooq.Engine
import com.github.supermariolabs.spooq.model.json.ProcessingOutputEncoder
import com.github.supermariolabs.spooq.model.{ProcessingOutput, Report, Step}
import org.slf4j.LoggerFactory
import spark.{Request, Response, Spark}
import io.circe._
import org.apache.spark.sql.DataFrame
//import io.circe.generic.semiauto._
import io.circe.generic.auto._
import io.circe.parser._
import io.circe.syntax._
import org.apache.spark.sql.SparkSession
import scala.util.{Try,Success,Failure}

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

    /*Spark.post("/sql", (req: Request, res: Response) => {
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
    })*/

    /**
     * If collect is false, will be created a new df with a temp view using id as name, otherwise will be only showed
     * the query output in json format
     * Request Example: curl -X POST localhost:4242/sql -d '{"id":"test1","sql":"SELECT * FROM helloGeo", "collect":false}'
     */
    Spark.post("/sql", (req: Request, res: Response) => {
      //implicit val sqlRequestDecoder: Decoder[SqlRequest] = deriveDecoder[SqlRequest]
      val decodedSql: Either[Error, SqlRequest] = decode[SqlRequest](req.body)
      res.`type`("application/json")

      decodedSql match {
        case Right(s) =>
          val id = s.id
          val out: Try[DataFrame] = Try(spark.sql(s.sql))
          val collect = s.collect.getOrElse(true)

          out match {
            case Success(df) =>
              res.status(200)
              if (collect) {
                s"""{"id":"$id", "res":[${out.map(_.toJSON.collect()).get.mkString(",")}]}"""
              } else {
                engine.dataFrames.put(id, df)
                df.createOrReplaceTempView(id)
                s"""{"id":"$id", "res":[TempView created correctly]}"""
              }

            case Failure(e) =>
              res.status(404)
              s"""{"id":"$id", "res":[${e.getMessage}]}"""
          }

        case Left(e) =>
          res.status(400)
          s"Bad Request! Error:${e.getMessage}"
      }
    })

    /**
     * Request Example: curl -X GET localhost:4242/unpersist/dataframeName
     */
    Spark.get("/unpersist/:name", (req: Request, res: Response) => {
      val id = req.params(":name")
      val isCached = engine.dataFrames.get(id) match {
        case Some(df) =>
          df.unpersist()
          df.storageLevel.useMemory.toString
        case None =>
          res.status(404)
          "error"
      }
      s"""{"id":"$id", "res":[httpStatus: ${res.status()}, isCached: $isCached]}"""
    })

    /**
     * Request Example: curl -X DELETE localhost:4242/step/dataframeName
     */
    Spark.delete("/step/:name", (req: Request, res: Response) => {
      val id = req.params(":name")
      val isDeleted = engine.dataFrames.get(id) match {
        case Some(df) =>
          spark.catalog.dropTempView(id)
        case None =>
          res.status(404)
          "error"
      }
      s"""{"id":"$id", "res":[httpStatus: ${res.status()}, deleted: $isDeleted]}"""
    })

    spark.catalog.dropTempView("df")

    /**
     * Request Example: curl -X GET localhost:4242/cache/dataframeName
     */
    Spark.get("/cache/:name", (req: Request, res: Response) => {
      val id = req.params(":name")
      val isCached = engine.dataFrames.get(id) match {
        case Some(df) =>
          df.cache()
          df.storageLevel.useMemory.toString

        case None =>
          res.status(404)
          "error"

      }
      s"""{"id":"$id", "res":[httpStatus: ${res.status()}, isCached: $isCached]}"""
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
