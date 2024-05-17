package com.github.supermariolabs.spooq

import com.github.supermariolabs.spooq.ansi.AnsiCodes
import com.github.supermariolabs.spooq.api.{HttpServer, SqlRequest}
import com.github.supermariolabs.spooq.conf.ApplicationConfiguration
import com.github.supermariolabs.spooq.etl.{CustomInputStep, SimpleStep}
import com.github.supermariolabs.spooq.logging.{CommonUtils, SparkUtils}
import com.github.supermariolabs.spooq.metrics.{LoggerListener, QueryLoggerListener}
import com.github.supermariolabs.spooq.misc.Utils
import com.github.supermariolabs.spooq.model._
import com.github.supermariolabs.spooq.shell.Repl
import com.github.supermariolabs.spooq.streaming.{SimpleForeachBatchProcessor, SimpleForeachProcessor}
import com.github.supermariolabs.spooq.udf.SimpleUDF
import com.github.supermariolabs.spooq.udf.hbase.HBaseEnrichRow
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.thriftserver.HiveThriftServer2
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{MapType, StringType}
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, Row, SparkSession, avro}
import org.slf4j.LoggerFactory
import scalaj.http.Http
import za.co.absa.abris.avro.functions
import za.co.absa.abris.config.AbrisConfig

import java.lang.reflect.InvocationTargetException
import java.util.UUID
import java.util.concurrent.Executors
import javax.script.ScriptEngineManager
import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success, Try}

class Engine(providedConf: ApplicationConfiguration) {

  val logger = LoggerFactory.getLogger(this.getClass)
  val conf = providedConf
  val ansi = new AnsiCodes(conf.rich.getOrElse(false))

  val dataFrames = scala.collection.mutable.Map[String, DataFrame]()
  val variables = new java.util.HashMap[String, Any]()
  val report = scala.collection.mutable.Map[String, Report]()

  var streaming = false
  var index = 0

  val knownKinds = Set(
    "input",
    "input-stream",
    "sql",
    "variable",
    "script",
    "script-v2",
    "custom",
    "customInput",
    "avro-serde",
    "udf",
    "output",
    "output-stream",
    "parse-json"
  )

  def run(job: Job): EngineOut = {
    val sparkSessionBuilder = SparkSession.builder.appName("Spooq")

    sys.env.getOrElse("SPARK_ENV_LOADED", false) match {
      case b: Boolean => {
        logger.info("SPARK_ENV_LOADED not found... using master=local[*]")
        if (!b) sparkSessionBuilder.master("local[*]")
      }
      case _ => {
        logger.info("SPARK_ENV_LOADED found!")
      }
    }

    // Experimental
    if (conf.thriftServer.getOrElse(false)) {
      sparkSessionBuilder.config("spark.sql.hive.thriftServer.singleSession", "true")
      sparkSessionBuilder.config("hive.server2.thrift.port", conf.thriftPort.getOrElse("10001"))
      //sparkSessionBuilder.config("javax.jdo.option.ConnectionURL", "jdbc:derby:;databaseName=metastore_db2;create=true").enableHiveSupport()
    }

    implicit val spark = sparkSessionBuilder.getOrCreate()
    logger.info(s"SparkSession created. Using Spark version ${spark.version}")
    spark.sparkContext.setLogLevel("WARN")
    spark.sparkContext.addSparkListener(new LoggerListener)
    spark.listenerManager.register(new QueryLoggerListener)

    logger.info(s"\n------------------------------\nJob: ${ansi.YELLOW}${job.id}${ansi.RESET} ${if (job.shortDesc.isDefined) s"[${job.shortDesc.get}]" else ""}\n------------------------------\n${job.desc.getOrElse("- No detailed description -")}")

    if (conf.enableGeo.getOrElse(false)) {
      logger.info("Loading GEO extensions (Apache Sedona SQL/MM Part3 Spatial SQL Standard)")
      Try(
        Class.forName("org.apache.sedona.sql.utils.SedonaSQLRegistrator").getMethod("registerAll", Class.forName("org.apache.spark.sql.SparkSession")).invoke(this, spark)
      ).getOrElse({
        logger.error("SedonaSQLRegistrator.registerAll FAILED!")
      })
    }


    job.steps.foreach(step => parseStep(step))

    // Experimental
    val runHttpServer = conf.http.getOrElse(false)
    if (runHttpServer) {
      ExecutionContext
        .fromExecutorService(Executors.newSingleThreadExecutor())
        .execute(() => {
          new HttpServer(this).run
        })
    }

    // Experimental
    if (conf.thriftServer.getOrElse(false)) {
      logger.info(s"[Experimental] Starting Thrift Server...")
      HiveThriftServer2.startWithContext(spark.sqlContext)
    }

    if (conf.interactive.getOrElse(false)) {
      val repl = new Repl(spark, conf.rich.getOrElse(false))
      repl.run
    }

    if (streaming) {
      logger.info("\nWaiting for the stream to complete...")
      while (streaming) {
        //wait
      }
    }

    while (runHttpServer) {
      //wait
    }

    EngineOut(dataFrames.toMap, variables.asInstanceOf[java.util.Map[String, Any]], report.toMap)
  }

  def parseStep(step: Step)(implicit spark: SparkSession) = {
    var t0 = System.nanoTime

    Try {
      index += 1
      var exitStatus = false
      logger.info(
        s"""
           |------------------------------
           |Step: ${ansi.YELLOW}${step.id}${ansi.RESET} ${if (step.shortDesc.isDefined) s"[${step.shortDesc.get}]" else ""}
           |------------------------------
           |${step.desc.getOrElse("- No detailed description -")}
           |${CommonUtils.dumpStep(step)}------------------------------""".stripMargin)

      var dependencyCheck = true
      step.dependsOn.foreach(dependencyList => {
        dependencyList.foreach(dependency => {
          dependencyCheck = (dependencyCheck && dataFrames.get(dependency).isDefined)
        })
      })
      if (!dependencyCheck) throw new IllegalStateException("dependencyCheck failed")

      t0 = System.nanoTime()

      if (knownKinds.contains(step.kind)) {
        spark.sparkContext.setLocalProperty("callSite.short", s"[step ${step.id + "] " + step.shortDesc.getOrElse("-")}")
        spark.sparkContext.setLocalProperty("callSite.long", s"[step ${step.id + "] " + step.desc.getOrElse("-")}")

        this.getClass.getMethod(s"process${Utils.functionByKind(step.kind)}", classOf[Step], classOf[SparkSession]).invoke(this, step, spark).asInstanceOf[ProcessingOutput]
      }
      else false
    } match {
      case Success(exitStatus) => {
        if (exitStatus.asInstanceOf[ProcessingOutput].exit) logger.info(s"Elapsed ${(System.nanoTime() - t0) / 1000000000} second(s)")
        report.put(step.id, Report(index, exitStatus.asInstanceOf[ProcessingOutput], None))
      }
      case Failure(exception) => {
        val ex = if (exception.isInstanceOf[InvocationTargetException]) exception.getCause else exception
        if (conf.verbose.isDefined && conf.verbose.toOption.get) logger.error(s"${ansi.RED}ERROR: ${ex.toString}${ansi.RESET}", ex)
        else logger.error(s"${ansi.RED}ERROR: ${ex.toString}${ansi.RESET}")
        val reason = if (ex.getMessage.contains("dependencyCheck failed")) ex.getMessage else ex.toString
        report.put(step.id, Report(index, ProcessingOutput(step, List.empty[LogEntry], None, false), Some(reason)))
      }
    }
    report.get(step.id)
  }

  def processInput(in: Step)(implicit spark: SparkSession): ProcessingOutput = {
    var reader = spark.read
    var customFormat: Either[Boolean, String] = Left(false)
    val customFormatVars = scala.collection.mutable.Map[String, String]()

    in.format.foreach(formatDefined => {
      formatDefined match {
        case "spooq-hbase" => {
          customFormat = Right("spooq-hbase")
          var nameSpace = "default"
          var tableName = "spooq"
          reader = reader.format("org.apache.hadoop.hbase.spark")
          in.options.foreach(optionDefined => {
            optionDefined.filter(_._1.startsWith("spooq.hbase.")).foreach(option => {
              if (option._1 == "spooq.hbase.table") {
                val spooqHBaseTable = option._2.split(":")
                if (spooqHBaseTable.length > 1) {
                  nameSpace = spooqHBaseTable(0)
                  tableName = spooqHBaseTable(1)
                } else {
                  tableName = spooqHBaseTable(0)
                }
                val catalog =
                  s"""{
                     |        "table": { "namespace":"$nameSpace", "name":"$tableName" },
                     |        "rowkey":"key",
                     |        "columns":{
                     |                "id":{"col":"key", "type":"binary"}
                     |        }}""".stripMargin
                logger.info(s"Configuring connection to HBase table `$nameSpace:$tableName`")
                reader = reader.option("catalog", catalog)
                customFormatVars.put("spooq.hbase.nameSpace", nameSpace)
                customFormatVars.put("spooq.hbase.tableName", tableName)
              }
            })
          })
        }
        case _ => {
          reader = reader.format(formatDefined)
        }
      }
    })

    in.options.foreach(optionDefined => {
      optionDefined.foreach(option =>
        reader = reader.option(option._1, option._2))
    })

    in.schema.foreach(schemaDefined => {
      reader = reader.schema(schemaDefined)
    })

    var df = spark.emptyDataFrame

    in.path match {
      case Some(path) => {
        if (path.startsWith("http")) {
          if (in.format.get == "json") {
            val httpClient = HttpClients.createSystem()
            val httpGet = new HttpGet(path)
            val response = httpClient.execute(httpGet)
            val entity = response.getEntity
            val data = EntityUtils.toString(entity)
            val stringDS = spark.createDataset(data.split("\n"))(Encoders.STRING)
            df = spark.read
              .option("multiline", "true")
              .option("header", "true")
              .json(stringDS)
          }
          else if (in.format.get == "csv") {
            val httpClient = HttpClients.createSystem()
            val httpGet = new HttpGet(path)
            val response = httpClient.execute(httpGet)
            val entity = response.getEntity
            val data = EntityUtils.toString(entity)
            val stringDS = spark.createDataset(data.split("\n"))(Encoders.STRING)
            df = spark.read
              .option("inferSchema", "true")
              .option("header", "true")
              .csv(stringDS)
          }
        }
        else {
          df = reader.load(path)
        }
      }
      case None => df = reader.load
    }

    customFormat match {
      case Left(b) => {
        /*nothing to do*/
      }
      case Right(format) => {
        format match {
          case "spooq-hbase" => {
            val hbaseUDF = new HBaseEnrichRow
            spark.udf.register("hbaseRow", hbaseUDF.udf)

            val tmpViewName = s"spooqHBase${UUID.randomUUID().toString.replace("-", "")}"
            df.createOrReplaceTempView(tmpViewName)

            df = spark.sql(
              s"""
                 |select rowKey, col.* from (select id as rowKey, explode(hbaseRow('${customFormatVars.get("spooq.hbase.nameSpace").getOrElse("")}:${customFormatVars.get("spooq.hbase.tableName").getOrElse("")}', id)) from $tmpViewName)
                 |""".stripMargin)
          }
          case _ => {
            /*nothing to do*/
          }
        }
      }
    }

    in.cache.foreach(cacheDefined => {
      if (cacheDefined) {
        logger.info(s"${ansi.YELLOW}Caching data${ansi.RESET}")
        df = df.cache
      }
    })

    dataFrames.put(in.id, df)
    if(in.isGlobal.getOrElse(false))
      df.createOrReplaceGlobalTempView(in.id)
    else
      df.createOrReplaceTempView(in.id)

    in.show.foreach(showDefined => {
      if (showDefined) logger.info(s"${ansi.CYAN}SAMPLE DATA${ansi.RESET}\n${SparkUtils.dfAsString(df)}")
    })

    ProcessingOutput(in, List.empty[LogEntry], None, true)
  }

  def processInputStream(in: Step)(implicit spark: SparkSession): ProcessingOutput = {
    var reader = spark.readStream
    in.format.foreach(formatDefined =>
      reader = reader.format(formatDefined))
    in.options.foreach(optionDefined => {
      optionDefined.foreach(option =>
        reader = reader.option(option._1, option._2))
    })

    var df = spark.emptyDataFrame

    in.schema.foreach(schemaDefined => {
      reader = reader.schema(schemaDefined)
    })

    in.path match {
      case Some(path) => df = reader.load(path)
      case None => df = reader.load
    }

    dataFrames.put(in.id, df)
    if(in.isGlobal.getOrElse(false))
      df.createOrReplaceGlobalTempView(in.id)
    else
      df.createOrReplaceTempView(in.id)

    in.show.foreach(showDefined => {
      if (showDefined) df.writeStream
        .format("console")
        .start
    })

    streaming = true
    ProcessingOutput(in, List.empty[LogEntry], None, true)
  }

  def processSql(trx: Step)(implicit spark: SparkSession): ProcessingOutput = {
    var df = spark.emptyDataFrame
    var sqlToProcess = ""

    /*trx.watermark.foreach(watermarkDefined => {
      val column = watermarkDefined.get("column").get
      val delay = watermarkDefined.get("delay").get

      df = df.withWatermark(column, delay)
    })*/

    trx.sql.foreach(sqlDefined => {
      sqlToProcess = templating.CommonUtils.processString(sqlDefined, variables.asInstanceOf[java.util.Map[String, Any]])
      logger.info(s"\nExecuting '${ansi.GREEN}${sqlToProcess.toUpperCase()}${ansi.RESET}'")

      //if (trx.watermark.isDefined) df = new SQLTransformer().setStatement(sqlToProcess).transform(df)
      /*else*/ df = spark.sql(sqlToProcess)
    })

    trx.cache.foreach(cacheDefined => {
      if (cacheDefined) {
        logger.info(s"${ansi.YELLOW}Caching data${ansi.RESET}")
        df = df.cache
      }
    })

    dataFrames.put(trx.id, df)
    if(trx.isGlobal.getOrElse(false))
      df.createOrReplaceGlobalTempView(trx.id)
    else
      df.createOrReplaceTempView(trx.id)

    trx.show.foreach(showDefined => {
      if (showDefined) logger.info(s"${ansi.CYAN}SAMPLE DATA${ansi.RESET}\n${SparkUtils.dfAsString(df)}")
    })

    if (conf.verbose.toOption.getOrElse(false)) df.printSchema

    ProcessingOutput(trx, List.empty[LogEntry], None, true)
  }

  def processVariable(trx: Step)(implicit spark: SparkSession): ProcessingOutput = {
    var df = spark.emptyDataFrame
    trx.sql.foreach(sqlDefined => {
      logger.info(s"\nExecuting '${ansi.GREEN}${sqlDefined.toUpperCase()}${ansi.RESET}'")
      df = spark.sql(sqlDefined)
    })

    val collect = df.collect
    if (collect.size == 1) {
      val variable = if (collect(0).isInstanceOf[GenericRowWithSchema]) collect(0).mkString else collect(0)
      variables.put(trx.id, variable)
    }
    else {
      variables.put(trx.id, collect)
    }

    trx.show.foreach(showDefined => {
      if (showDefined) logger.info(s"\n${ansi.CYAN}VARIABLE DUMP${ansi.RESET}\n${variables.get(trx.id)} [${variables.get(trx.id).getClass.getName}]")
    })

    ProcessingOutput(trx, List.empty[LogEntry], None, true)
  }

  def processScriptV2(script: Step)(implicit spark: SparkSession): ProcessingOutput = {
    logger.info("+++ DEBUG AMMONITE +++")
    ammonite.Main(predefCode =
      s"""
         |logger.info("+++ INSIDE SCRIPT spark.version="+spark.version)
         |""".stripMargin).run("logger" -> logger, "spark" -> spark)

    ProcessingOutput(script, List.empty[LogEntry], None, true)
  }

  def processScript(script: Step)(implicit spark: SparkSession): ProcessingOutput = {
    val manager = new ScriptEngineManager()
    val engineName = script.jsr223Engine.getOrElse("scala")
    val engine = manager.getEngineByName(engineName)

    logger.info(s"Using engine: ${engine.getFactory.getEngineName} ver. ${engine.getFactory.getEngineVersion}")

    engineName match {
      case "scala" => {
        engine.put("sparkTmp", spark)
        engine.put("scTmp", spark.sparkContext)
        engine.put("loggerTmp", logger)
        dataFrames.foreach(df => {
          engine.put(s"${df._1}Tmp", df._2)
        })

        script.code.foreach(snippet => {
          var initSnippet =
            """
              |val spark = sparkTmp.asInstanceOf[org.apache.spark.sql.SparkSession]
              |val sc = scTmp.asInstanceOf[org.apache.spark.SparkContext]
              |val logger = loggerTmp.asInstanceOf[org.slf4j.Logger]
              |
              |""".stripMargin
          dataFrames.foreach(df => {
            initSnippet += s"val ${df._1} = ${df._1}Tmp.asInstanceOf[org.apache.spark.sql.DataFrame]\n"
          })
          val ret = engine.eval(initSnippet + snippet)
          logger.info(s"Engine out: ${ret.getClass.getName}")
          ret match {
            case df: DataFrame => {
              dataFrames.put(script.id, df)
              if(script.isGlobal.getOrElse(false))
                df.createOrReplaceGlobalTempView(script.id)
              else
                df.createOrReplaceTempView(script.id)
            }
            case _ => logger.error(s"Wrong script return format (${ret.getClass.getName})")
          }
        })
      }
      case _ => {
        engine.put("spark", spark)
        engine.put("sc", spark.sparkContext)
        engine.put("logger", logger)
        dataFrames.foreach(df => {
          engine.put(df._1, df._2)
        })

        script.code.foreach(snippet => {
          val ret = engine.eval(snippet)
          logger.info(s"Engine out: ${ret.getClass.getName}")
          ret match {
            case df: DataFrame => {
              dataFrames.put(script.id, df)
              if(script.isGlobal.getOrElse(false))
                df.createOrReplaceGlobalTempView(script.id)
              else
                df.createOrReplaceTempView(script.id)
            }
            case _ => logger.error(s"Wrong script return format (${ret.getClass.getName})")
          }
        })
      }
    }

    ProcessingOutput(script, List.empty[LogEntry], None, true)
  }

  def processOutput(out: Step)(implicit spark: SparkSession): ProcessingOutput = {
    var df = dataFrames.get(out.source.get).get

    out.repartition.foreach(repartitionDefined => {
      df = df.repartition(repartitionDefined.map(col(_)): _*)
    })

    var writer = df.write
    out.format.foreach(formatDefined =>
      writer = writer.format(formatDefined))
    out.options.foreach(optionDefined => {
      optionDefined.foreach(option =>
        writer = writer.option(option._1, option._2))
    })
    out.mode.foreach(modeDefined => {
      writer.mode(modeDefined)
    })
    out.partitionBy.foreach(partitionByDefined => {
      writer = writer.partitionBy(partitionByDefined: _*)
    })

    logger.info(s"\nSaving '${ansi.GREEN}${out.source.get}${ansi.RESET}' ${if (out.path.isDefined) s"to '${ansi.YELLOW}${out.path.get}${ansi.RESET}'" else ""}")

    out.path match {
      case Some(path) => writer.save(path)
      case None => writer.save
    }

    ProcessingOutput(out, List.empty[LogEntry], None, true)
  }

  def processOutputStream(out: Step)(implicit spark: SparkSession): ProcessingOutput = {
    var options = "-"
    out.options.foreach(o => {
      options = o.map(kv => s"${kv._1} -> ${kv._2}").mkString(", ")
    })
    logger.info(s"Processing '${out.id}' (kind: ${out.kind}, format: ${out.format.getOrElse("-")}, options: [$options])")

    var outDF = dataFrames.get(out.source.get).get

    /*out.watermark.foreach(watermarkDefined => {
      val column = watermarkDefined.get("column").get
      val delay = watermarkDefined.get("delay").get

      outDF = outDF.withWatermark(column, delay)
    })*/

    var writer = outDF.writeStream

    out.format.foreach(formatDefined => {
      formatDefined match {
        case "foreach" => {
          logger.info(s"Custom processing (foreach) using ${out.claz.getOrElse("")}")
          val options = out.options.getOrElse(Map.empty[String, String]).filter(opt => opt._1.startsWith("spooq.foreach."))
          val claz = Class.forName(out.claz.getOrElse("")).getConstructors.last.newInstance(options).asInstanceOf[SimpleForeachProcessor]
          writer = writer.foreach(claz.processor)
        }
        case "foreachBatch" => {
          logger.info(s"Custom processing (foreachBatch) using... using ${out.claz.getOrElse("")}")
          val options = out.options.getOrElse(Map.empty[String, String]).filter(opt => opt._1.startsWith("spooq.foreach."))
          val claz = Class.forName(out.claz.getOrElse("")).getConstructors.last.newInstance(options).asInstanceOf[SimpleForeachBatchProcessor]
          writer = writer.foreachBatch(claz.processor)
        }
        case _ => writer = writer.format(formatDefined)
      }
    })


    out.options.foreach(optionDefined => {
      optionDefined.foreach(option =>
        writer = writer.option(option._1, option._2))
    })

    out.partitionBy.foreach(partitionByDefined => {
      writer = writer.partitionBy(partitionByDefined: _*)
    })

    out.outputMode.foreach(outputModeDefined => {
      writer = writer.outputMode(outputModeDefined)
    })

    out.trigger.foreach(triggerDefined => {
      triggerDefined.get("policy").foreach(policy => {
        policy match {
          case "processingTime" => {
            val timeSpec = triggerDefined.get("value").getOrElse("5 seconds")
            writer = writer.trigger(Trigger.ProcessingTime(timeSpec))
          }
          case "once" => {
            writer = writer.trigger(Trigger.Once())
          }
          case "continuous" => {
            val timeSpec = triggerDefined.get("value").getOrElse("1 second")
            writer = writer.trigger(Trigger.Continuous(timeSpec))
          }
          case _ => {
            logger.warn("Invalid trigger policy... skipping")
          }
        }
      })
    })

    writer.start

    ProcessingOutput(out, List.empty[LogEntry], None, true)
  }

  def processCustom(custom: Step)(implicit spark: SparkSession): ProcessingOutput = {
    val className = custom.claz.get
    val claz = Class.forName(className).newInstance.asInstanceOf[SimpleStep]

    var df = claz.run(dataFrames.toMap, variables.asScala.toMap, conf.params)

    custom.cache.foreach(cacheDefined => {
      if (cacheDefined) {
        logger.info(s"${ansi.YELLOW}Caching data${ansi.RESET}")
        df = df.cache
      }
    })

    dataFrames.put(custom.id, df)
    if(custom.isGlobal.getOrElse(false))
      df.createOrReplaceGlobalTempView(custom.id)
    else
      df.createOrReplaceTempView(custom.id)

    custom.show.foreach(showDefined => {
      if (showDefined) logger.info(s"${ansi.CYAN}SAMPLE DATA${ansi.RESET}\n${SparkUtils.dfAsString(df)}")
    })

    ProcessingOutput(custom, List.empty[LogEntry], None, true)
  }

  def processCustomInput(custom: Step)(implicit spark: SparkSession): ProcessingOutput = {
    val className = custom.claz.get
    val claz = Class.forName(className).newInstance.asInstanceOf[CustomInputStep]

    var df = claz.run(dataFrames.toMap, variables.asScala.toMap, conf.params, custom)

    custom.cache.foreach(cacheDefined => {
      if (cacheDefined) {
        logger.info(s"${ansi.YELLOW}Caching data${ansi.RESET}")
        df = df.cache
      }
    })

    dataFrames.put(custom.id, df)
    if(custom.isGlobal.getOrElse(false))
      df.createOrReplaceGlobalTempView(custom.id)
    else
      df.createOrReplaceTempView(custom.id)

    custom.show.foreach(showDefined => {
      if (showDefined) logger.info(s"${ansi.CYAN}SAMPLE DATA${ansi.RESET}\n${SparkUtils.dfAsString(df)}")
    })

    ProcessingOutput(custom, List.empty[LogEntry], None, true)
  }

  def processAvroSerde(serde: Step)(implicit spark: SparkSession): ProcessingOutput = {
    var df = dataFrames.get(serde.source.get).get
    var schemaRegistry = "mock://fake-registry"
    serde.options.foreach(optDefined => {
      schemaRegistry = optDefined.get("schema.registry.url").getOrElse("mock://fake-registry")
    })

    serde.mode.foreach(modeDefined => {
      modeDefined match {
        case "decode" => {
          serde.columns.foreach(columnList => {
            columnList.foreach(column => {
              column match {
                case Column(_, _, Some("vanilla"), _, _, _, Some(schema)) => {
                  df = df.withColumn(column.name.get, functions.from_avro(col(column.name.get), schema))
                }
                case Column(_, _, Some("confluent"), Some("id"), Some(params), _, _) => {
                  val schemaId = column.schemaStrategyParams.get.head.toInt
                  val config = AbrisConfig
                    .fromConfluentAvro
                    .downloadReaderSchemaById(schemaId)
                    .usingSchemaRegistry(schemaRegistry)

                  df = df.withColumn(column.name.get, functions.from_avro(col(column.name.get), config))
                }
                case Column(_, _, Some("confluent"), Some("topicKey"), Some(params), _, _) => {
                  val topicName = column.schemaStrategyParams.get.head
                  val config = AbrisConfig
                    .fromConfluentAvro
                    .downloadReaderSchemaByLatestVersion
                    .andTopicNameStrategy(topicName, isKey = true)
                    .usingSchemaRegistry(schemaRegistry)

                  df = df.withColumn(column.name.get, functions.from_avro(col(column.name.get), config))
                }
                case Column(_, _, Some("confluent"), Some("topicValue"), Some(params), _, _) => {
                  val topicName = column.schemaStrategyParams.get.head
                  val config = AbrisConfig
                    .fromConfluentAvro
                    .downloadReaderSchemaByLatestVersion
                    .andTopicNameStrategy(topicName, isKey = false)
                    .usingSchemaRegistry(schemaRegistry)

                  df = df.withColumn(column.name.get, functions.from_avro(col(column.name.get), config))
                }
                case Column(_, _, Some("confluent"), Some("record"), Some(params), _, _) => {
                  val (recordName, namespace) = column.schemaStrategyParams.getOrElse(None) match {
                    case List(r: String, n: String) => (r, n)
                    case _ => ("undefinedRecordName", "undefinedNamespace")
                  }
                  val config = AbrisConfig
                    .fromConfluentAvro
                    .downloadReaderSchemaByLatestVersion
                    .andRecordNameStrategy(recordName, namespace)
                    .usingSchemaRegistry(schemaRegistry)

                  df = df.withColumn(column.name.get, functions.from_avro(col(column.name.get), config))
                }
                case Column(_, _, Some("confluent"), Some("topicAndRecord"), Some(params), _, _) => {
                  val (topic, recordName, namespace) = column.schemaStrategyParams.getOrElse(None) match {
                    case List(t: String, r: String, n: String) => (t, r, n)
                    case _ => ("undefinedTopic", "undefinedRecordName", "undefinedNamespace")
                  }
                  val config = AbrisConfig
                    .fromConfluentAvro
                    .downloadReaderSchemaByLatestVersion
                    .andTopicRecordNameStrategy(topic, recordName, namespace)
                    .usingSchemaRegistry(schemaRegistry)

                  df = df.withColumn(column.name.get, functions.from_avro(col(column.name.get), config))
                }
                case _ => logger.error(s"Inconsistent parameters: '$column'")
              }
            })
          })
        }
        case "encode" => {
          serde.columns.foreach(columnList => {
            columnList.foreach(column => {
              column match {
                case Column(Some(name), None, Some("vanilla"), _, _, _, _) => {
                  column.schema match {
                    case Some(schema) => df = df.withColumn(name, functions.to_avro(col(name), schema))
                    case None => df = df.withColumn(name, avro.functions.to_avro(col(name)))
                  }
                }
                case Column(Some(name), Some(names), Some("vanilla"), _, _, _, _) => {
                  column.schema match {
                    case Some(schema) => {
                      val allColumns = struct(df.columns.filter(c => names.contains(c)).map(c => df.col(c)): _*)
                      df = df.withColumn(name, functions.to_avro(allColumns, schema))
                    }
                    case None => {
                      val allColumns = struct(df.columns.filter(c => names.contains(c)).map(c => df.col(c)): _*)
                      df = df.withColumn(name, avro.functions.to_avro(allColumns))
                    }
                  }
                }
                case Column(Some(name), None, Some("confluent"), Some("id"), Some(params), _, _) => {
                  val schemaId = column.schemaStrategyParams.get.head.toInt
                  val config = AbrisConfig
                    .toConfluentAvro
                    .downloadSchemaById(schemaId)
                    .usingSchemaRegistry(schemaRegistry)

                  df = df.withColumn(column.name.get, functions.to_avro(col(column.name.get), config))
                }
                case Column(Some(name), Some(names), Some("confluent"), Some("id"), Some(params), _, _) => {
                  val schemaId = column.schemaStrategyParams.get.head.toInt
                  val config = AbrisConfig
                    .toConfluentAvro
                    .downloadSchemaById(schemaId)
                    .usingSchemaRegistry(schemaRegistry)

                  val allColumns = struct(df.columns.filter(c => names.contains(c)).map(c => df.col(c)): _*)
                  df = df.withColumn(column.name.get, functions.to_avro(allColumns, config))
                }
                case Column(Some(name), None, Some("confluent"), Some("topicKey"), Some(params), _, _) => {
                  val topicName = column.schemaStrategyParams.get.head
                  val config = AbrisConfig
                    .toConfluentAvro
                    .downloadSchemaByLatestVersion
                    .andTopicNameStrategy(topicName, isKey = true)
                    .usingSchemaRegistry(schemaRegistry)

                  df = df.withColumn(column.name.get, functions.to_avro(col(column.name.get), config))
                }
                case Column(Some(name), Some(names), Some("confluent"), Some("topicKey"), Some(params), _, _) => {
                  val topicName = column.schemaStrategyParams.get.head
                  val config = AbrisConfig
                    .toConfluentAvro
                    .downloadSchemaByLatestVersion
                    .andTopicNameStrategy(topicName, isKey = true)
                    .usingSchemaRegistry(schemaRegistry)

                  val allColumns = struct(df.columns.filter(c => names.contains(c)).map(c => df.col(c)): _*)
                  df = df.withColumn(column.name.get, functions.to_avro(allColumns, config))
                }
                case Column(Some(name), None, Some("confluent"), Some("topicValue"), Some(params), _, _) => {
                  val topicName = column.schemaStrategyParams.get.head
                  val config = AbrisConfig
                    .toConfluentAvro
                    .downloadSchemaByLatestVersion
                    .andTopicNameStrategy(topicName, isKey = false)
                    .usingSchemaRegistry(schemaRegistry)

                  df = df.withColumn(column.name.get, functions.to_avro(col(column.name.get), config))
                }
                case Column(Some(name), Some(names), Some("confluent"), Some("topicValue"), Some(params), _, _) => {
                  val topicName = column.schemaStrategyParams.get.head
                  val config = AbrisConfig
                    .toConfluentAvro
                    .downloadSchemaByLatestVersion
                    .andTopicNameStrategy(topicName, isKey = false)
                    .usingSchemaRegistry(schemaRegistry)

                  val allColumns = struct(df.columns.filter(c => names.contains(c)).map(c => df.col(c)): _*)
                  df = df.withColumn(column.name.get, functions.to_avro(allColumns, config))
                }
                case Column(Some(name), None, Some("confluent"), Some("record"), Some(params), _, _) => {
                  val (recordName, namespace) = column.schemaStrategyParams.getOrElse(None) match {
                    case List(r: String, n: String) => (r, n)
                    case _ => ("undefinedRecordName", "undefinedNamespace")
                  }
                  val config = AbrisConfig
                    .toConfluentAvro
                    .downloadSchemaByLatestVersion
                    .andRecordNameStrategy(recordName, namespace)
                    .usingSchemaRegistry(schemaRegistry)

                  df = df.withColumn(column.name.get, functions.to_avro(col(column.name.get), config))
                }
                case Column(Some(name), Some(names), Some("confluent"), Some("record"), Some(params), _, _) => {
                  val (recordName, namespace) = column.schemaStrategyParams.getOrElse(None) match {
                    case List(r: String, n: String) => (r, n)
                    case _ => ("undefinedRecordName", "undefinedNamespace")
                  }
                  val config = AbrisConfig
                    .toConfluentAvro
                    .downloadSchemaByLatestVersion
                    .andRecordNameStrategy(recordName, namespace)
                    .usingSchemaRegistry(schemaRegistry)

                  val allColumns = struct(df.columns.filter(c => names.contains(c)).map(c => df.col(c)): _*)
                  df = df.withColumn(column.name.get, functions.to_avro(allColumns, config))
                }
                case Column(Some(name), None, Some("confluent"), Some("topicAndRecord"), Some(params), _, _) => {
                  val (topic, recordName, namespace) = column.schemaStrategyParams.getOrElse(None) match {
                    case List(t: String, r: String, n: String) => (t, r, n)
                    case _ => ("undefinedTopic", "undefinedRecordName", "undefinedNamespace")
                  }
                  val config = AbrisConfig
                    .toConfluentAvro
                    .downloadSchemaByLatestVersion
                    .andTopicRecordNameStrategy(topic, recordName, namespace)
                    .usingSchemaRegistry(schemaRegistry)

                  df = df.withColumn(column.name.get, functions.to_avro(col(column.name.get), config))
                }
                case Column(Some(name), Some(names), Some("confluent"), Some("topicAndRecord"), Some(params), _, _) => {
                  val (topic, recordName, namespace) = column.schemaStrategyParams.getOrElse(None) match {
                    case List(t: String, r: String, n: String) => (t, r, n)
                    case _ => ("undefinedTopic", "undefinedRecordName", "undefinedNamespace")
                  }
                  val config = AbrisConfig
                    .toConfluentAvro
                    .downloadSchemaByLatestVersion
                    .andTopicRecordNameStrategy(topic, recordName, namespace)
                    .usingSchemaRegistry(schemaRegistry)

                  val allColumns = struct(df.columns.filter(c => names.contains(c)).map(c => df.col(c)): _*)
                  df = df.withColumn(column.name.get, functions.to_avro(allColumns, config))
                }
                case _ => logger.error(s"Inconsistent parameters: '$column'")
              }
            })
          })
        }
        case _ => logger.error(s"Unknown mode: '$modeDefined'")
      }
    })


    dataFrames.put(serde.id, df)
    if(serde.isGlobal.getOrElse(false))
      df.createOrReplaceGlobalTempView(serde.id)
    else
      df.createOrReplaceTempView(serde.id)

    ProcessingOutput(serde, List.empty[LogEntry], None, true)
  }

  def processUdf(udf: Step)(implicit spark: SparkSession): ProcessingOutput = {
    val className = udf.claz.get
    logger.info("LOG UDF ERROR +++ " + Class.forName(className).getConstructors.last)
    println("LOG UDF ERROR +++ " + Class.forName(className).getConstructors.last)
    val claz = Class.forName(className).getConstructors.last.newInstance().asInstanceOf[SimpleUDF]
    spark.udf.register(udf.id, claz.udf)

    ProcessingOutput(udf, List.empty[LogEntry], None, true)
  }

  def processParseJson(in: Step)(implicit spark: SparkSession): ProcessingOutput = {
    import spark.implicits._
    val sourceDf = in.source.getOrElse("json")
    val df = dataFrames(sourceDf)
    var column = "default"
    in.options.foreach(optionsDefined =>
      column = optionsDefined.getOrElse("column", column))

    //cast json column into MapType
    val dfMapType = df.select(explode(col(column)).as("json"))
      .withColumn("json", from_json(col("json"), MapType(StringType, StringType)))
    //take all Map keys to generate columns
    val keysDF = dfMapType.select(explode(map_keys($"json"))).distinct()
    val keys = keysDF.collect().map(_.getString(0))
    val keyCols = keys.map(k => col("json").getItem(k).as(k))
    var out = dfMapType.select(keyCols: _*)

    in.cache.foreach(cacheDefined => {
      if (cacheDefined) {
        logger.info(s"${ansi.YELLOW}Caching data${ansi.RESET}")
        out = out.cache
      }
    })

    dataFrames.put(in.id, out)
    if(in.isGlobal.getOrElse(false))
      out.createOrReplaceGlobalTempView(in.id)
    else
      out.createOrReplaceTempView(in.id)

    in.show.foreach(showDefined => {
      if (showDefined) logger.info(s"${ansi.CYAN}SAMPLE DATA${ansi.RESET}\n${SparkUtils.dfAsString(df)}")
    })
    ProcessingOutput(in, List.empty[LogEntry], None, true)
  }

}
