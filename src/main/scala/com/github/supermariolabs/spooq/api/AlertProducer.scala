package com.github.supermariolabs.spooq.api

import com.github.javafaker.Address
import com.github.supermariolabs.spooq.etl.SimpleStep
import com.google.gson.Gson
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Encoders, SaveMode, SparkSession}
import org.slf4j.LoggerFactory

import java.net.{URI, URLEncoder}
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters.asScalaBufferConverter

case class Address(city : String, county: String, state: String, country : String)
case class SubSystemInfo(xUid:String, address: Address)
case class Position(`type` : String, coordinates: Array[Double])
case class AlertInfo(position: Position,probable_cause: String, severity: String, ruleId: String, additional_info: Array[Double])
case class Alert(`date` : String,displayName: String,alertInfo: AlertInfo, subSystemInfo: SubSystemInfo)
case class AlarmBody(bodyType: String, alert: Alert )
case class Coordinates (lan : Double, lon: Double)


case class Alarm(header: Header, body: AlarmBody)

class AlertProducer extends SimpleStep {
  val logger = LoggerFactory.getLogger(this.getClass)



  def getLocationCoordinates(location: String, spark: SparkSession, uri: String): Position = {
    import spark.implicits._
    val encoded = URLEncoder.encode(location, "UTF-8")
    //val path = "http://x2030-gis-geo-code-dev.westeurope.cloudapp.azure.com/api/search?provider=nominatim&query=" + encoded
    val path = uri+encoded

    val httpClient = HttpClients.createDefault()
    val httpGet = new HttpGet(path)
    val response = httpClient.execute(httpGet)
    val entity = response.getEntity
    val data = EntityUtils.toString(entity)
    val stringDS = spark.createDataset(data.split("\n"))(Encoders.STRING)
    val df = spark.read.json(stringDS)
    val coordinates = df.filter(col("address.displayName").ilike("%"+location+"%")).select("center.coordinates").limit(1)

    coordinates.map(x => Coordinates(x.get(0).asInstanceOf[mutable.WrappedArray[Double]](0), x.get(0).asInstanceOf[mutable.WrappedArray[Double]](1))).limit(1).collectAsList().asScala.toList match{
      case coordinates  :: _ => Position("Point", List(coordinates.lan, coordinates.lon).toArray)
      case _ => Position("Point", List(0.0,0.0).toArray)
    }
  }


  override def run(dfMap: Map[String, DataFrame], varMap: Map[String, Any], args : Map[String,String]): DataFrame = {
    val spark = SparkSession.getActiveSession.get
    import org.apache.kafka.clients.producer._

    import java.util.Properties
    //kafka conf
    val props = new Properties()
    props.put("bootstrap.servers", args("kserver"))
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](props)
    val TOPIC_ALARM = args("ktopicAlarm")


    val alarmsList = ListBuffer(dfMap("kpiBySta"),dfMap("kpiByMun"),dfMap("kpiByProv"),dfMap("kpiByReg"))
    val items = new ListBuffer[String]
    alarmsList.foreach(dataFrame => {
      dataFrame.collect().foreach( alarm => {

        val id = alarm.get(0).toString
        val mun = alarm.getString(3)
        val prov = alarm.getString(5)
        val reg = alarm.getString(6)

        var pos = Position("Point", List(0.0,0.0).toArray)
        if(prov == "NA" && mun == "all" && id == "0"){
          pos = getLocationCoordinates(reg,spark,args("geoUri"))
        }
        else if (prov != "NA" && mun == "all" && id == "0") {
          pos = getLocationCoordinates(prov, spark,args("geoUri"))
        }
        else if(prov != "NA" && mun != "all" && id == "0"){
          pos = getLocationCoordinates(mun,spark,args("geoUri"))
        }else{
          val aux = Coordinates(alarm.getDouble(1), alarm.getDouble(2))
          pos = Position("Point", List(aux.lan, aux.lon).toArray)
        }


        val details = Alarm(
          Header("0", "HISTORICAL_DATA_MANAGER", "NA", (new java.util.Date).toString, "notification", "medium", List(), "1.0", "MessageHeader"),
          AlarmBody("HDMAlarmNotification",
            Alert((new java.util.Date).toString,"displayName",
              AlertInfo(pos,"probableCause",alarm.getString(11),alarm.getString(7),List(alarm.getDouble(8),alarm.getDouble(9),alarm.getDouble(10)).toArray),
              SubSystemInfo(id, Address(mun,prov,reg,"Italy")))))

        val gson = new Gson
        val json = gson.toJson(details)
        items.append(json)
      })
    })

    items.foreach( item => {
      val alert = new ProducerRecord(TOPIC_ALARM, "key", item)
      producer.send(alert)
    })

    producer.close()

    spark.emptyDataFrame
  }
}