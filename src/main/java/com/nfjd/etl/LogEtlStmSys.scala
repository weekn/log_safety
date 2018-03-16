package com.nfjd.etl
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
import org.apache.spark.sql.SparkSession

//com.nfjd.etl.LogEtlStmSys
object LogEtlStmSys {

  def main(args: Array[String]) {
    val configUtil=new ConfigUtil()
    configUtil.readConfig()

   	val conf = new SparkConf().setAppName("SAFE-log-etl")//.setMaster("local[2]")
    conf.set("es.nodes", configUtil.es_serves).set("es.port", "9300")
    conf.set("es.index.auto.create", "true")
    conf.set("spark.streaming.stopGracefullyOnShutdown","true")
    //conf.set("spark.streaming.backpressure.enabled","true")      //开启后spark自动根据系统负载选择最优消费速率
    conf.set("spark.streaming.backpressure.initialRate","10000")      //限制第一次批处理应该消费的数据，因为程序冷启动队列里面有大量积压，防止第一次全部读取，造成系统阻塞
    conf.set("spark.streaming.kafka.maxRatePerPartition","3000")      //限制每秒每个消费线程读取每个kafka分区最大的数据量
    conf.set("spark.streaming.receiver.maxRate","10000")      //设置每次接收的最大数量
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")   //使用Kryo序列化
//    conf.set("spark.scheduler.mode","FAIR")
//    conf.set("spark.streaming.concurrentJobs","2")
   //	conf.setExecutorEnv(variable, value)
    val ssc = new StreamingContext(conf, Seconds(5))

    val kafkaParams = Map[String, Object](
     "bootstrap.servers" -> configUtil.kafka_spark_servers, //"172.17.17.21:9092,172.17.17.22:9092",
      // "bootstrap.servers" -> "172.17.17.21:9092,172.17.17.22:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "safelog_etl_01",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean))

    
    val bc_pattern = ssc.sparkContext.broadcast(configUtil.patterns)

    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](Array("safeLog"), kafkaParams))
   
    stream.foreachRDD(rdd => {
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      
      try {
        dealLog(bc_pattern, rdd)

        stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
      } catch {

        case e: Exception => {
//          val kafkaProducer = new KafkaProducerUtil(configUtil.log_servers,configUtil.log_topic)
//          kafkaProducer.send("error", e.getMessage)
//          kafkaProducer.close()
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
        SyslogProcess.run(patterns_bro.value, log)
       
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
    //rr.collect().foreach(println)
    
   
		
    //rdd.coalesce(3)
    
   
    rr.saveToEs("{es_index}/{es_type}")
   
		
  }

}