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
		val conf = new SparkConf().setAppName("networklog-etl") //.setMaster("local[2]")
		conf.set("es.nodes", configUtil.es_serves).set("es.port", "9300")
		conf.set("es.index.auto.create", "true")
		//conf.set("es.batch.size.entries", "1")
		conf.set("spark.streaming.stopGracefullyOnShutdown", "true")
		//conf.set("spark.streaming.backpressure.enabled","true")      //开启后spark自动根据系统负载选择最优消费速率
		//conf.set("spark.streaming.backpressure.initialRate", "10000") //限制第一次批处理应该消费的数据，因为程序冷启动队列里面有大量积压，防止第一次全部读取，造成系统阻塞
		conf.set("spark.streaming.kafka.maxRatePerPartition", "80") //限制每秒每个消费线程读取每个kafka分区最大的数据量
		conf.set("spark.streaming.receiver.maxRate", "80") //设置每次接收的最大数量
		conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer") //使用Kryo序列化
//    conf.set("spark.scheduler.mode","FAIR")
//    conf.set("spark.streaming.concurrentJobs","3")

		val ssc = new StreamingContext(conf, Seconds(10))

		val kafkaParams = Map[String, Object](
			"bootstrap.servers" -> configUtil.kafka_spark_servers, //"172.17.17.21:9092,172.17.17.22:9092",
			// "bootstrap.servers" -> "172.17.17.21:9092,172.17.17.22:9092",
			"key.deserializer" -> classOf[StringDeserializer],
			"value.deserializer" -> classOf[StringDeserializer],
			"group.id" -> "networklog-etl0",
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
				dealLogUseMapPartition(bc_pattern, rdd)

				stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
			} catch {

				case e: Exception => {
//					val kafkaProducer = new KafkaProducerUtil(configUtil.log_servers, configUtil.log_topic)
//					kafkaProducer.send("error", e.getMessage)
//					kafkaProducer.close()
					e.printStackTrace()
				}
			}
		})

		ssc.start()
		ssc.awaitTermination()
	}

	def readFlowlogRule(): ArrayBuffer[Map[String, String]] = {
		val conn = PgSqlUtil.getPGConnection()
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
		//rdd.mapPartitions(f, preservesPartitioning)

		val rr = rdd.flatMap(record => {

			val log = record.value()
			try {
				if (log.split("netlog_http").length > 1) {
					FlowProcess.run(log, flowlogRule)
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
	
	def dealLogUseMapPartition(flowlogRule: Broadcast[ArrayBuffer[Map[String, String]]], rdd: RDD[ConsumerRecord[String, String]]): Unit = {

		val rr = rdd.mapPartitions(iter => {
			val producer2netflow = new KafkaProducerUtil( "netflow")
			val producer2flow = new KafkaProducerUtil( "flow")
			val res_seq = scala.collection.mutable.ListBuffer.empty[scala.collection.mutable.Map[String, Any]]
			
			var n_i=0
			while (iter.hasNext) {
				n_i=n_i+1
				val log = iter.next().value()
				
				try {
					if (log.split("""netlog_http|netlog_bd""").length > 1) {
						
						FlowProcess.run(log, flowlogRule).foreach(map => {
							res_seq += map
						
							producer2flow.send(null,
									"id="+map.getOrElse("id", "null")+","+
									"uri="+map.getOrElse("uri", "null")+","+
									"dstip="+map.getOrElse("dstip", "null")+","+
									"srcip="+map.getOrElse("srcip", "null")+","+
									"date="+map.getOrElse("date", "null"))
						})
					} else {
						
						NetflowProcess.run(log).foreach(map => {
							res_seq += map
							producer2netflow.send(null, map.toString())
						})
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

				}
			}
			println("size= "+n_i)
			producer2netflow.close()
			
			res_seq.toIterator
		})
		//rr.foreach(println)
		rr.saveToEs("{es_index}/{es_type}")
	}

}