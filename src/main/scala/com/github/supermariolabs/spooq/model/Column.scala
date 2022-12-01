package com.github.supermariolabs.spooq.model

case class Column(
                   var name: scala.Option[String],
                   var names: scala.Option[List[String]],
                   var avroType: scala.Option[String],
                   var schemaStrategy: scala.Option[String],
                   var schemaStrategyParams: scala.Option[List[String]],
                   var registerSchema: scala.Option[Boolean],
                   var schema: scala.Option[String]
                 )