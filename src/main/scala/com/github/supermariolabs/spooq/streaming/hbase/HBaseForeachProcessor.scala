package com.github.supermariolabs.spooq.streaming.hbase

import com.github.supermariolabs.spooq.streaming.SimpleForeachProcessor
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.{ForeachWriter, Row}
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

class HBaseForeachProcessor(options: Map[String, String] = Map.empty[String, String]) extends SimpleForeachProcessor {
  val logger = LoggerFactory.getLogger(this.getClass)
  var tableName = "spooq"
  var sourceShape = "wide"
  var rowKey = "id"
  var cf = "spq"

  options.foreach(opt => {
    logger.info(s"${opt._1} -> ${opt._2}")
    opt._1 match {
      case "spooq.foreach.hbase.tableName" => tableName = opt._2
      case "spooq.foreach.hbase.sourceShape" => {
        sourceShape = opt._2
        if (sourceShape == "wide") cf = "-"
      }
      case "spooq.foreach.hbase.cf" => cf = opt._2
      case "spooq.foreach.hbase.rowKey" => rowKey = opt._2
      case _ => logger.warn(s"Unknown option: ${opt._1}")
    }
  })

  logger.info(s"HBaseForeachProcessor init (tableName=$tableName, sourceShape=$sourceShape, rowKey=$rowKey , cf=$cf)")

  override val processor: ForeachWriter[Row] = new ForeachWriter[Row] {
    override def open(partitionId: Long, epochId: Long): Boolean = {
      logger.info(s"SampleForeachProcessor::open($partitionId, $epochId)")
      true
    }

    override def process(row: Row): Unit = {
      logger.info(s"SampleForeachProcessor::process($row)")
      var rowStr = ""
      row.schema.foreach(field => {
        rowStr += (s", ${field.name} [${field.dataType.sql}] -> ${row.getAs(field.name)}")
      })
      logger.info("Row: " + rowStr.substring(2))

      val table = getConnection.getTable(TableName.valueOf(Bytes.toBytes(tableName)))
      sourceShape match {
        case "wide" => {
          row.schema.filter(field => field.name != rowKey).foreach(field => {
            insert(table,
              row.getAs[Array[Byte]](rowKey),
              (
                cf.getBytes,
                field.name.getBytes,
                row.getAs[Array[Byte]](field.name)
              ) :: Nil
            )
          })
        }
        case "long" => {
          insert(table,
            row.getAs[Array[Byte]](rowKey),
            (
              row.getAs[Array[Byte]]("cf"),
              row.getAs[Array[Byte]]("cq"),
              row.getAs[Array[Byte]]("value")
            ) :: Nil
          )
        }
        case _ => logger.warn(s"Unknown sourceShape: ${sourceShape}")
      }
    }

    override def close(errorOrNull: Throwable): Unit = {
      if (errorOrNull != null) logger.error(s"SampleForeachProcessor::close(${errorOrNull.getMessage})")
    }
  }

  def getConnection(): Connection = {
    Try {
      val threadLocal: ThreadLocal[Connection] = new ThreadLocal[Connection]();
      var conn = threadLocal.get
      if (conn == null || conn.isClosed || conn.isAborted) {
        conn = ConnectionFactory.createConnection
        threadLocal.set(conn)
      }
      conn
    } match {
      case Success(conn) => conn
      case Failure(f) => ConnectionFactory.createConnection
    }
  }

  def insert(table: Table, rowKey: Array[Byte], cells: List[(Array[Byte], Array[Byte], Array[Byte])]): Unit = {
    val put = new Put(rowKey)
    cells.foreach(x => put.addColumn(x._1, x._2, x._3))

    table.put(put)
  }
}
