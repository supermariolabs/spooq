package com.github.supermariolabs.spooq.udf.mongodb

import com.github.supermariolabs.spooq.udf.SimpleUDF
import com.mongodb.client.MongoClients
import com.mongodb.client.model.Projections.include
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions
import org.bson.Document

import scala.collection.Map
import scala.collection.mutable.ListBuffer

class MongoQuery(options: Map[String, String] = Map.empty[String, String]) extends SimpleUDF{

  def query(bson: String, filter: Option[List[String]] = None): List[String] = {
    val uri = options("spark.mongodb.read.connection.uri")
    val mongoClient = MongoClients.create(uri)
    val coll = mongoClient.getDatabase(options("spark.mongodb.read.database")).getCollection(options("spark.mongodb.read.collection"))
    val res: ListBuffer[String] = ListBuffer()
    filter match {
      case Some(columns) => coll.find(Document.parse(bson)).projection(include(columns:_*)).iterator.forEachRemaining(doc => res.append(doc.toJson))
      case None => coll.find(Document.parse(bson)).iterator.forEachRemaining(doc => res.append(doc.toJson))
    }
    mongoClient.close()
    res.toList
  }

  override val udf: UserDefinedFunction = functions.udf(query(_:String, _:Option[List[String]]))
}
