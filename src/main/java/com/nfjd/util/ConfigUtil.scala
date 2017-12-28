package com.nfjd.util
import scala.xml.XML
import com.nfjd.model.RegPattern
class ConfigUtil {
  var kafka_spark_servers: String= _
  var kafka_spark_topics: Array[String]= _
  var log_topic: String= _
  var log_servers: String= _
  var patterns: Seq[RegPattern]= _
  var es_serves:String= _
  def readConfig(): Unit = {
    
    val xml = XML.loadFile("config.xml")
    val kafka_spark_topics = (xml \ "kafka_spark_topics" \ "topic").map(x => x.text).toArray
    val kafka_spark_servers = (xml \ "kafka_spark_servers").text
    val log_topic = (xml \ "log_topic").text
    val log_servers = (xml \ "log_servers").text
     val es_serves= (xml \ "es_serves").text
    val patterns = (xml \ "patterns" \ "pattern").map(x => {
   
      RegPattern(id = (x \ "@id").text, pattern = (x \ "content").text, fields = (x \ "field").map(s => {
        ((s \ "@id").text, (s \ "@group").text)
      }))
    })

    this.kafka_spark_topics = kafka_spark_topics
    this.kafka_spark_servers = kafka_spark_servers
    this.log_topic = log_topic
    this.log_servers = log_servers
    this.patterns = patterns
    this.es_serves=es_serves

  }
}