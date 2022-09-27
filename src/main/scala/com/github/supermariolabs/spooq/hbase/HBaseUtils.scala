package com.github.supermariolabs.spooq.hbase

import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put, Table}

import scala.util.{Failure, Success, Try}

object HBaseUtils {
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
