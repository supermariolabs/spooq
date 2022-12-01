package com.github.supermariolabs.spooq

import com.github.supermariolabs.spooq.ansi.AnsiCodes
import org.slf4j.LoggerFactory
import io.circe.parser.decode
import io.circe.generic.auto._
import com.github.supermariolabs.spooq.conf.ApplicationConfiguration
import com.github.supermariolabs.spooq.logging.JobUtils
import com.github.supermariolabs.spooq.model.{EngineOut, Job}

import java.io.{File, StringReader, StringWriter}
import java.util
import javax.script.{ScriptEngineFactory, ScriptEngineManager}
import scala.collection.immutable.ListMap
import scala.io.Source
import scala.util.{Failure, Properties, Success, Try}
import collection.JavaConverters._

object Application {
  val logger = LoggerFactory.getLogger(this.getClass)
  var ansi: AnsiCodes = _

  def main(args: Array[String]): Unit = {
    Try {
      val conf = new ApplicationConfiguration(args)
      ansi = new AnsiCodes(conf.rich.getOrElse(false))

      run(conf)
    } match {
      case Success(run) =>
      case Failure(exception) =>
        logger.error(s"Something gone wrong: ${exception.toString}")
      }
    }

  def run(configFile: String): Option[EngineOut] = {
    run(new ApplicationConfiguration("--configuration-file"::configFile::Nil))
  }

  def run(config: String, format: String, inline: Boolean=false): Option[EngineOut] = {
    if (inline) run(new ApplicationConfiguration("--configuration-string"::config::"-f"::format::Nil))
    else run(new ApplicationConfiguration("--configuration-file"::config::"-f"::format::Nil))
  }

  def run(conf: ApplicationConfiguration): Option[EngineOut] = {
    ansi = new AnsiCodes(conf.rich.getOrElse(false))
    Try {
      printBanner()
      logger.info(s"\n${conf.summary.replace("Scallop", s"${conf.getClass.getSimpleName}")}")

      var inline = false
      conf.configurationFile.toOption.foreach(cf => {
        inline = false
      })
      conf.configurationString.toOption.foreach(cs=>{
        inline = true
      })

      val job = parseJobFile(conf.configurationFile.toOption.getOrElse(conf.configurationString.toOption.get), conf.format.toOption, inline)

      val jobUtils = new JobUtils(job)
      jobUtils.dump(job)

      val engine = new Engine(conf)
      engine.run(job)
    } match {
      case Success(out) => {
        logger.info(s"All done! :-)")
        var reportStr = ""
        ListMap(out.report.toSeq.sortBy(_._2.index):_*).foreach(r => {
          reportStr+=s"\n[${r._2.index}] ${r._1} -> ${if (r._2.done.exit) s"${ansi.GREEN}SUCCESS${ansi.RESET}" else if (r._2.reason.isDefined && r._2.reason.get.contains("dependencyCheck failed")) s"${ansi.YELLOW}SKIPPED${ansi.RESET}" else s"${ansi.RED}FAILED${ansi.RESET}"}"
          r._2.reason.foreach(msg=>reportStr+=s" (${msg.toString})")
        })
        logger.info(s"\n-------------\n${ansi.YELLOW}FINAL REPORT${ansi.RESET}\n-------------$reportStr")
        Some(out)
      }
      case Failure(exception) => {
        logger.error(s"Something gone wrong: ${exception.toString}", exception)
        None
      }
    }
  }

  def printBanner(): Unit = {
    val banner = Source.fromInputStream(getClass.getClassLoader.getResourceAsStream("banner.txt")).mkString
    val build = Source.fromInputStream(getClass.getClassLoader.getResourceAsStream("lastbuild")).mkString
    logger.info(s"\n${ansi.BOLD}${banner.replace("$$BUILD$$",build)}${ansi.RESET}")
    logger.info(s"Using Scala ${Properties.versionString} (${System.getProperty("java.vm.name")}, Java ${System.getProperty("java.version")})")

    logger.info(s"--------------")
    logger.info(s"JSR223 support")
    logger.info(s"--------------")
    val factories = new ScriptEngineManager().getEngineFactories
    factories.asScala.foreach(factory => {
      val engName = factory.getEngineName
      val engVersion = factory.getEngineVersion
      val langName = factory.getLanguageName
      val langVersion = factory.getLanguageVersion
      val engNames = factory.getNames
      var aliases = ""
      engNames.asScala.foreach(name => {
        aliases+=s",$name"
      })
      logger.info(s"Script Engine: $engName  ver. $engVersion [${aliases.substring(1)}], Language: $langName ver. $langVersion");
    })

  }

  def parseJobFile(cfgFile: String, format: Option[String]=None, inline: Boolean=false): Job = {
    println(s"[parseJobFile] inline: $inline")
    val knownFormats = ("hocon"::"conf"::"json"::"yaml"::"yml"::Nil).toSet[String]
    var job: Job = Job("-",None,None,List.empty)

    val fileFormat = format.getOrElse({
      logger.info("Trying to understand the format of the configuration file...")
      val foundFormat = new File(cfgFile).getName.split("\\.").last.toLowerCase
      if (!knownFormats.contains(foundFormat)) {
        throw new Exception(s"File format unknown: $foundFormat")
      }
      foundFormat
    })
    logger.info(s"File format: $fileFormat")

    val jobCfg = decode[Job](jsonString(cfgFile, fileFormat, inline))

    jobCfg match {
      case Right(j) => {
        logger.info(s"Configuration file correctly read and parsed")
        job = j
      }
      case Left(e) => {
        exit(s"Parsing error [ ${e.getMessage} ]")
      }
    }
    job
  }

  def jsonString(cfgFile: String, format: String, inline: Boolean=false): String = {
    println(s"[jsonString] inline: $inline")
    var out = ""
    val in = if (inline) cfgFile else scala.io.Source.fromFile(cfgFile).mkString

    format match {
      case "json" => {
        out = in
      }
      case "yaml" | "yml" => {
        import io.circe.yaml
        val decoded = yaml.parser.parse(in)
        decoded match {
          case Right(str) => out=str.toString
          case Left(err) => out=""
        }
      }
      case "hocon" | "conf" => {
        import io.circe.config
        val decoded = config.parser.parse(in)
        decoded match {
          case Right(str) => out=str.toString
          case Left(err) => out=""
        }
      }
      case _ => logger.error(s"Unsupported configuration file format: $format")
    }

    parseVariables(out)
  }

  def parseVariables(in: String): String = {
    var out = in
    Try {
      val env = System.getenv()
      val sys = System.getProperties

      val cfg = new freemarker.template.Configuration(freemarker.template.Configuration.VERSION_2_3_0)
      cfg.setInterpolationSyntax(freemarker.template.Configuration.DOLLAR_INTERPOLATION_SYNTAX)
      val objectWrapper = (new freemarker.template.DefaultObjectWrapperBuilder(freemarker.template.Configuration.VERSION_2_3_0))
      cfg.setObjectWrapper(objectWrapper.build())

      val model = new util.HashMap[String,Object]()
      model.put("env",env)
      model.put("sys",sys)

      val t = new freemarker.template.Template("spooq", new StringReader(in), cfg);

      val tmp = new StringWriter()
      t.process(model, tmp)

      out = tmp.toString
    } match {
      case Success(out) => logger.info(s"Parsed out:\n$out")
      case Failure(ex) => logger.error(s"Parsing error: ${ex.getMessage}")
    }
    out
  }

  def exit(msg: String): Unit = {
    logger.error(s"The program will be closed prematurely: $msg")
    System.exit(125)
  }
}
