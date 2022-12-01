package com.github.supermariolabs.spooq.model

case class Step(
               var id: String,
               //var avro: scala.Option[Boolean],
               //var avroSchema: scala.Option[String],
               var avroSchemas: scala.Option[Map[String,String]],
               var shortDesc: scala.Option[String],
               var desc: scala.Option[String],
               var kind: String,
               var schema: scala.Option[String],
               var jsr223Engine: scala.Option[String],
               var format: scala.Option[String],
               var claz: scala.Option[String],
               var code: scala.Option[String],
               var function: scala.Option[String],
               var options: scala.Option[Map[String,String]],
               var sql: scala.Option[String],
               //var watermark: scala.Option[Map[String,String]],
               var columns: scala.Option[List[Column]],
               var source: scala.Option[String],
               var path: scala.Option[String],
               var mode: scala.Option[String],
               var repartition: scala.Option[List[String]],
               var partitionBy: scala.Option[List[String]],
               var table: scala.Option[String],
               var outputMode: scala.Option[String],
               var trigger: scala.Option[Map[String,String]],
               var dependsOn: scala.Option[List[String]],
               var show: scala.Option[Boolean],
               var cache: scala.Option[Boolean]
               ) {
  implicit def reflector(ref: AnyRef) = new {
    def getField(name: String): Any = ref.getClass.getMethods.find(_.getName == name).get.invoke(ref)
    def setField(name: String, value: Any): Unit = ref.getClass.getMethods.find(_.getName == name + "_$eq").get.invoke(ref, value.asInstanceOf[AnyRef])
  }

  /*def preProcess() : Step = {
    val fields = this.getClass.getDeclaredFields.toList.map(_.getName)
    val values = this.productIterator.toSeq
    fields.zip(values).foreach(kv => {
      kv._2 match {
        case s:   String                            => s
        case os:  scala.Option[String]              => os
        case ls:  List[String]                      => ls
        case ols: scala.Option[List[String]]        => ols
        case ms:  Map[String, String]               => ms
        case oms: scala.Option[Map[String, String]] => oms
        case b:   Boolean                           => b
        case ob:  scala.Option[Boolean]             => ob
        case _                                      => kv._1
      }
    })
    this
  }*/
}
