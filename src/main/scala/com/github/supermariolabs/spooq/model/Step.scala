package com.github.supermariolabs.spooq.model

case class Step(
               var id: String,
               var kind: String,
               var avro: scala.Option[Boolean] = None,
               var avroSchema: scala.Option[String] = None,
               var avroSchemas: scala.Option[Map[String,String]] = None,
               var shortDesc: scala.Option[String] = None,
               var desc: scala.Option[String] = None,
               var schema: scala.Option[String] = None,
               var jsr223Engine: scala.Option[String] = None,
               var format: scala.Option[String] = None,
               var claz: scala.Option[String] = None,
               var code: scala.Option[String] = None,
               var function: scala.Option[String] = None,
               var options: scala.Option[Map[String,String]] = None,
               var sql: scala.Option[String] = None,
               var source: scala.Option[String] = None,
               var path: scala.Option[String] = None,
               var mode: scala.Option[String] = None,
               var repartition: scala.Option[List[String]] = None,
               var partitionBy: scala.Option[List[String]] = None,
               var table: scala.Option[String] = None,
               var outputMode: scala.Option[String] = None,
               var trigger: scala.Option[Map[String,String]] = None,
               var dependsOn: scala.Option[List[String]] = None,
               var show: scala.Option[Boolean] = None,
               var cache: scala.Option[Boolean] = None
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
