package com.nfjd
import scala.util.matching.Regex


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
import com.nfjd.util.ConfigUtil

//com.nfjd.LogEtlStm
object LogEtlStm {

  def main(args: Array[String]) {
    val configUtil=new ConfigUtil()
    configUtil.readConfig()
    val conf = new SparkConf().setAppName("nfjd-log-etl").setMaster("local[2]")
    conf.set("es.nodes", configUtil.es_serves).set("es.port", "9200")
    val ssc = new StreamingContext(conf, Seconds(1))

    val kafkaParams = Map[String, Object](
     "bootstrap.servers" -> configUtil.kafka_spark_servers, //"172.17.17.21:9092,172.17.17.22:9092",
      // "bootstrap.servers" -> "172.17.17.21:9092,172.17.17.22:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "1",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean))

    
    val bc_pattern = ssc.sparkContext.broadcast(configUtil.patterns)

    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](configUtil.kafka_spark_topics, kafkaParams))

    stream.foreachRDD(rdd => {
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      
      try {
        dealLog(bc_pattern, rdd)

        stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
      } catch {

        case e: Exception => {
          val kafkaProducer = new KafkaProducerUtil(configUtil.log_servers,configUtil.log_topic)
          kafkaProducer.send("error", e.getMessage)
          kafkaProducer.close()
          e.printStackTrace()
        }
      }
    })

    ssc.start()
    ssc.awaitTermination()
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