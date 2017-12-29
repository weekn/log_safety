package com.nfjd.etl
import scala.util.matching.Regex
import scala.xml.XML

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.elasticsearch.spark.streaming.sparkDStreamFunctions
import org.elasticsearch.spark.streaming.sparkStringJsonDStreamFunctions
import org.json4s.Extraction
import org.json4s.JObject
import org.json4s.JsonDSL.list2jvalue
import org.json4s.ShortTypeHints
import org.json4s.jackson.JsonMethods.compact
import org.json4s.jackson.JsonMethods.parse
import org.json4s.jackson.JsonMethods.render
import org.json4s.jackson.Serialization
import org.json4s.jvalue2monadic
import org.json4s.string2JsonInput
import com.nfjd.model.RegPattern
//com.nfjd.LogEtl
object LogEtl {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("nfjd-log-etl").set("es.nodes", "172.17.17.30").set("es.port", "9200").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(1))

    val kafkaParams = Map[String, Object](
      // "bootstrap.servers" -> "192.168.181.234:9092", //"172.17.17.21:9092,172.17.17.22:9092",
      "bootstrap.servers" -> "172.17.17.21:9092,172.17.17.22:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "1",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean))
    //val topics = Array("flow_demo", "netflow_demo","syslog_safety_demo","zwj_test")
    val (topics, patterns) = readConfig()
    val broadcastVar = ssc.sparkContext.broadcast(patterns)

    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams))
    val data = stream.map(record => record.value)

    dealFlow(data)
    dealNetFlow(data)
    dealSysLog(patterns, data)
    ssc.start()
    ssc.awaitTermination()
  }
  def readConfig(): (Array[String], Seq[RegPattern]) = {
     val xml = XML.loadFile("config.xml")
    val topics = (xml \ "kafka_topics" \ "topic").map(x => x.text).toArray

    val patterns = (xml \ "patterns" \ "pattern").map(x => {

      RegPattern(id=(x\"@id").text,pattern = (x \ "content").text, fields = (x \ "field").map(s => {
        ((s \ "@id").text, (s \ "@group").text)
      }))
    })
    (topics, patterns)
  }

  def dealSysLog(patterns: Seq[RegPattern], data: DStream[String]): Unit = {

    for (pattern <- patterns) {

      val rr = data.map(line => {
        // broadcastVar.value
        val reg = new Regex(pattern.pattern)
        reg.findFirstMatchIn(line) match {
          case Some(s) => {
            var res_map: Map[String, String] = Map("test" -> "test")
            for (field <- pattern.fields) {
              res_map = res_map + (field._1 -> s.group(Integer.parseInt(field._2)))
            }
            res_map
          }
          case None => "none"
        }
      }).filter(x => x != "none")
      rr.saveToEs("spark_test/syslog")

      rr.print()
    }
  }
  def dealFlow(data: DStream[String]): Unit = {

    data.filter(line => {
      val pattern = """netlog_http.\{.*\}""".r
      pattern.findFirstIn(line) match {
        case Some(s) => true
        case None => false
      }
    }).map(line => {
      val pattern = new Regex("""netlog_http.(\{.*\})""")
      pattern.findFirstMatchIn(line).get.group(1)
    }).saveJsonToEs("spark_test/flow")
  }

  def dealNetFlow(lines: DStream[String]): Unit = {
    lines.filter(f => {
      val pattern = """send a message:.\[(\[.*\])\]""".r
      pattern.findFirstIn(f) match {
        case Some(s) => true
        case None => false
      }
    }).map(line => {
      val pattern = new Regex("""send a message:.\[(\[.*\])\]""")
      val res = pattern.findFirstMatchIn(line)
      res.get.group(1)
    }).flatMap(x => {
      val json_obj = parse(x, useBigDecimalForDouble = true)
      for {
        JObject(child) <- json_obj

      } yield compact(render(child))

    }).saveJsonToEs("spark_test/netflow")
  }

 
}