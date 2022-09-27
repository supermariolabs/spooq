package com.github.supermariolabs.spooq.udf.hbase

import com.github.supermariolabs.spooq.hbase.HBaseUtils
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
    val table = HBaseUtils.getConnection.getTable(TableName.valueOf( Bytes.toBytes(tableName) ) )
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

  override val udf: UserDefinedFunction = functions.udf(enrich(_:String,_:Array[Byte]))
}
