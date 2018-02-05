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
import scala.collection.mutable.ArrayBuffer
import java.sql.DriverManager
import java.sql.ResultSet
import com.nfjd.util.PgSqlUtil

//com.nfjd.etl.LogEtlStm
object LogEtlStm {

	def main(args: Array[String]) {
		val configUtil = new ConfigUtil()
		configUtil.readConfig()
		val conf = new SparkConf().setAppName("nfjd-log-etl") //.setMaster("local[2]")
		conf.set("es.nodes", configUtil.es_serves).set("es.port", "9300")
		conf.set("es.index.auto.create", "true")
		//conf.set("es.batch.size.entries", "1")
		conf.set("spark.streaming.stopGracefullyOnShutdown", "true")
		//conf.set("spark.streaming.backpressure.enabled","true")      //开启后spark自动根据系统负载选择最优消费速率
		conf.set("spark.streaming.backpressure.initialRate", "10000") //限制第一次批处理应该消费的数据，因为程序冷启动队列里面有大量积压，防止第一次全部读取，造成系统阻塞
		conf.set("spark.streaming.kafka.maxRatePerPartition", "2000") //限制每秒每个消费线程读取每个kafka分区最大的数据量
		conf.set("spark.streaming.receiver.maxRate", "10000") //设置每次接收的最大数量
		conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer") //使用Kryo序列化
		//    conf.set("spark.scheduler.mode","FAIR")
		//    conf.set("spark.streaming.concurrentJobs","3")

		val ssc = new StreamingContext(conf, Seconds(5))

		val kafkaParams = Map[String, Object](
			"bootstrap.servers" -> configUtil.kafka_spark_servers, //"172.17.17.21:9092,172.17.17.22:9092",
			// "bootstrap.servers" -> "172.17.17.21:9092,172.17.17.22:9092",
			"key.deserializer" -> classOf[StringDeserializer],
			"value.deserializer" -> classOf[StringDeserializer],
			"group.id" -> "log_etl",
			"auto.offset.reset" -> "latest",
			"enable.auto.commit" -> (false: java.lang.Boolean)
		)

		val bc_pattern = ssc.sparkContext.broadcast(readFlowlogRule())

		val stream = KafkaUtils.createDirectStream[String, String](
			ssc,
			PreferConsistent,
			Subscribe[String, String](configUtil.kafka_spark_topics, kafkaParams)
		)

		stream.foreachRDD(rdd => {
			val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
			//rdd.repartition(60)
			try {
				dealLog(bc_pattern, rdd)

				stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
			} catch {

				case e: Exception => {
					val kafkaProducer = new KafkaProducerUtil(configUtil.log_servers, configUtil.log_topic)
					kafkaProducer.send("error", e.getMessage)
					kafkaProducer.close()
					e.printStackTrace()
				}
			}
		})

		ssc.start()
		ssc.awaitTermination()
	}

	def readFlowlogRule(): ArrayBuffer[Map[String, String]] = {
		val conn=PgSqlUtil.getPGConnection()
		val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)
		var arr = new ArrayBuffer[Map[String, String]]()

		try {
			val rs = statement.executeQuery("SELECT * FROM public.flowlog_rule")
			var columnCount = rs.getMetaData().getColumnCount();

			while (rs.next) {
				
				val map = Map(
					"ruleid" -> rs.getArray("ruleid").toString(),
					"destip" -> rs.getArray("destip").toString(),
					"sourceip" -> rs.getArray("sourceip").toString(),
					"rule_regular_expression" -> rs.getArray("rule_regular_expression").toString()
				)
				arr += map
				//        for (i <- 1 to columnCount) {
				//          print(rs.getString(i) + "\t");
				//        }
				//        println();
			}
			println(arr)
		} finally {
			conn.close
		}
		arr
	}
	def dealLog(flowlogRule: Broadcast[ArrayBuffer[Map[String, String]]], rdd: RDD[ConsumerRecord[String, String]]): Unit = {

		val rr = rdd.flatMap(record => {

			val log = record.value()
			try {
				if (log.split("netlog_http").length > 1) {
					FlowProcess.run(log,flowlogRule)
				} else {
					NetflowProcess.run(log)
				}

			} catch {
				case e: Exception =>
					{

						val e_map = Map(
							"time" -> TimeUtil.getTime(),
							"error" -> e.toString(),
							"stackTrace" -> e.getStackTraceString,
							"log" -> log
						)
						implicit val formats = Serialization.formats(NoTypeHints)
						throw new IllegalStateException(write(e_map))
					}
					List()
			}

		})
		//rr.take(100).foreach(println)
		//rr.coalesce(3)
		rr.saveToEs("{es_index}/{es_type}")
	}

}