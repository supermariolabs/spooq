package com.github.supermariolabs.spooq.udf.mongodb

import com.github.supermariolabs.spooq.udf.SimpleUDF
import com.mongodb.client.MongoClients
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions
import org.bson.Document

import scala.collection.mutable.ListBuffer

class MongoQuery extends SimpleUDF{

  def query(bson: String) = {
    val uri = "mongodb://"
    val coll = MongoClients.create(uri).getDatabase("").getCollection("")

    val res: ListBuffer[Document] = ListBuffer()
    coll.find(Document.parse(bson)).iterator.forEachRemaining(doc => res.append(doc))

    res.toList
  }

  override val udf: UserDefinedFunction = functions.udf(query(_:String))
}
