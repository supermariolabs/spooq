package com.github.supermariolabs.spooq.shell

import com.github.supermariolabs.spooq.ansi.AnsiCodes
import org.apache.spark.sql.SparkSession
import org.jline.reader.{LineReader, LineReaderBuilder}
import org.jline.terminal.TerminalBuilder
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

class Repl(spark: SparkSession, rich: Boolean=false) {

  val logger = LoggerFactory.getLogger(this.getClass)
  val ansi = new AnsiCodes(rich)

  var reader: Option[LineReader] = None
  val prompt = s"${ansi.CYAN}${ansi.BOLD}spooq${ansi.RESET} \b${ansi.YELLOW}>${ansi.RESET} "

  var defaultRows = 20
  var defaultTruncate = 20
  var defaultVertical = false

  def run(): Unit = {
    Try {
      reader = Some(LineReaderBuilder
        .builder
        .terminal(TerminalBuilder.builder().build())
        .build)
    } match {
      case Success(s) => logger.info("JLine reader initialized!")
      case Failure(f) => logger.error(s"JLine error: ${f.getMessage}")
    }

    var cmd = ""
    logger.info(s"\nType in expressions to have them evaluated.\nType :help for more information.\n")
    while (cmd != ":quit") {
      cmd = readCmd()
      Try {
        logger.info(s"EXECUTING '${ansi.YELLOW}${cmd.toUpperCase()}${ansi.RESET}'")
        parseCmd(cmd)
      } match {
        case Success(c) => {}
        case Failure(exception) => {
          logger.error(s"${ansi.RED}Error: ${exception.getMessage}${ansi.RESET}")
        }
      }
    }

    def readCmd(): String = {
      reader match {
        case Some(r) => r.readLine(prompt)
        case None => {
          print(prompt)
          scala.io.StdIn.readLine
        }
      }
    }

    def parseCmd(cmd: String): Unit = {
      val c = cmd.trim
      val parsed = c.split(" ")
      if (!c.startsWith(":")) spark.sql(c).show(numRows = defaultRows, truncate = defaultTruncate, vertical = defaultVertical)
      else c match {
        case quit if c.startsWith(":quit") => {}
        case help if c.startsWith(":help") => {
          logger.info("Available commands ':help', ':showRows', ':truncate', ':vertical', ':quit'")
        }
        case varDump if c.startsWith(":variables") => {
          // to be implemented
        }
        case showRows if c.startsWith(":showRows") => {
          if (parsed.size>1) defaultRows=parsed(1).toInt
          logger.info(s"Actual showRows: '${ansi.YELLOW}${defaultRows}${ansi.RESET}'")
        }
        case truncate if c.startsWith(":truncate") => {
          if (parsed.size>1) defaultTruncate=parsed(1).toInt
          logger.info(s"Actual truncate: '${ansi.YELLOW}${defaultTruncate}${ansi.RESET}'")
        }
        case vertical if c.startsWith(":vertical") => {
          if (parsed.size>1) defaultVertical=parsed(1).toBoolean
          logger.info(s"Actual truncate: '${ansi.YELLOW}${defaultVertical}${ansi.RESET}'")
        }
        case _ => logger.info(s"Command unrecognized or unsupported: $c")
      }
    }

  }

}
