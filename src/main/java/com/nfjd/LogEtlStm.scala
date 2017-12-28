package com.nfjd
import scala.util.matching.Regex
import scala.xml.XML

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.elasticsearch.spark.streaming.sparkDStreamFunctions
import org.elasticsearch.spark.streaming.sparkStringJsonDStreamFunctions
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization._

import com.nfjd.util.KafkaProducerUtil

import com.nfjd.model.RegPattern
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.elasticsearch.spark._
import com.nfjd.util.TimeUtil

//com.nfjd.LogEtlStm
object LogEtlStm {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("nfjd-log-etl").set("es.nodes", "172.17.17.30").set("es.port", "9200")//.setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(1))

    val kafkaParams = Map[String, Object](
      //"bootstrap.servers" -> "192.168.181.234:9092", //"172.17.17.21:9092,172.17.17.22:9092",
       "bootstrap.servers" -> "172.17.17.21:9092,172.17.17.22:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "1",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (true: java.lang.Boolean))

    val (topics, patterns) = readConfig()
    val broadcastVar = ssc.sparkContext.broadcast(patterns)

    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams))

    stream.foreachRDD(rdd => {
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      val kafkaProducer = new KafkaProducerUtil
      try {
        dealLog(broadcastVar, rdd)

        stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
      } catch {

        case e: Exception => {
          kafkaProducer.send("error", e.getMessage)
          e.printStackTrace()
        }
      } finally {
        kafkaProducer.close()
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }
  def readConfig(): (Array[String], Seq[RegPattern]) = {
    val xml = XML.loadFile("config.xml")
    val topics = (xml \ "kafka_topics" \ "topic").map(x => x.text).toArray

    val patterns = (xml \ "patterns" \ "pattern").map(x => {

      RegPattern(id = (x \ "@id").text, pattern = (x \ "content").text, fields = (x \ "field").map(s => {
        ((s \ "@id").text, (s \ "@group").text)
      }))
    })
    (topics, patterns)
  }

  def dealLog(patterns_bro: Broadcast[Seq[RegPattern]], rdd: RDD[ConsumerRecord[String, String]]): Unit = {

    val rr = rdd.flatMap(record => {
      val log = record.value()
      try {
        if (log.split("""\|\!""").length > 28) { //是否syslog
          SyslogProcess.run(patterns_bro.value, log)
        } else if (log.split("netlog_http").length > 1) {
          FlowProcess.run(log)
        } else {
          NetflowProcess.run(log)
        }
      }catch{
        case e: Exception => {
         
          val e_map=Map(
              "time"->TimeUtil.getTime(),
              "error"->e.toString(),
              "stackTrace"->e.getStackTraceString,
              "log"->log
              )
          implicit val formats = Serialization.formats(NoTypeHints)
          throw new IllegalStateException(write(e_map))
        }
        List()
      }

    })

    rr.saveToEs("spark_test/{es_type}")
  }

}