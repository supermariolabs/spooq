package com.github.supermariolabs.spooq.udf.hbase

import com.github.supermariolabs.spooq.udf.SimpleUDF
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Get}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{CellUtil, TableName}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions
import scala.collection._

import scala.util.{Failure, Success, Try}

class HBaseEnrichRow extends SimpleUDF {
  case class HBaseRecord(family: Array[Byte], column: Array[Byte], value: Array[Byte])

  def enrich(tableName: String, rowKey: Array[Byte]): List[HBaseRecord] = {
    val recordList = mutable.ListBuffer[HBaseRecord]()
    val table = getConnection.getTable(TableName.valueOf( Bytes.toBytes(tableName) ) )
    val get = new Get(rowKey);
    val res = table.get(get);
    res.rawCells().foreach(cell => {
      val family = CellUtil.cloneFamily(cell)
      val column = CellUtil.cloneQualifier(cell)
      val value = CellUtil.cloneValue(cell)
      recordList+=HBaseRecord(family,column,value)
    })
    recordList.toList
  }

  def getConnection(): Connection = {
    Try {
      val threadLocal: ThreadLocal[Connection] = new ThreadLocal[Connection]();
      var conn = threadLocal.get
      if (conn==null || conn.isClosed || conn.isAborted) {
        conn = ConnectionFactory.createConnection
        threadLocal.set(conn)
      }
      conn
    } match {
      case Success(conn) => conn
      case Failure(f) => ConnectionFactory.createConnection
    }
  }

  override val udf: UserDefinedFunction = functions.udf(enrich(_:String,_:Array[Byte]))
}
