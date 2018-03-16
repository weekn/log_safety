package com.nfjd.analyse.netflowAnalyse

import com.nfjd.util.ConfigUtil
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.Seconds
import org.apache.spark.ml.PipelineModel
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.sql.SQLContext
import scala.util.matching.Regex
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.DataFrame
import com.nfjd.util.PgSqlUtil
import java.sql.Timestamp
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext

//com.nfjd.analyse.netflowAnalyse.PredictOnline
object PredictOnline {
	def main(args: Array[String]): Unit = {
		val configUtil = new ConfigUtil()
		configUtil.readConfig()
		val conf = new SparkConf().setAppName("netflow_attack_predict_ss") //.setMaster("local[2]")
		conf.set("es.nodes", configUtil.es_serves).set("es.port", "9300")
		conf.set("es.index.auto.create", "true")
		//conf.set("es.batch.size.entries", "1")
		conf.set("spark.streaming.stopGracefullyOnShutdown", "true")
		conf.set("spark.streaming.kafka.maxRatePerPartition", "3000") //限制每秒每个消费线程读取每个kafka分区最大的数据量
		conf.set("spark.streaming.receiver.maxRate", "3000") //设置每次接收的最大数量

		val ssc = new StreamingContext(conf, Seconds(10))

		val kafkaParams = Map[String, Object](
			"bootstrap.servers" -> configUtil.kafka_spark_servers, //"172.17.17.21:9092,172.17.17.22:9092",
			"key.deserializer" -> classOf[StringDeserializer],
			"value.deserializer" -> classOf[StringDeserializer],
			"group.id" -> "netflow_attack_predict_ss_14",
			"auto.offset.reset" -> "latest",
			"enable.auto.commit" -> (false: java.lang.Boolean)
		)
		val model = PipelineModel.load("hdfs://172.17.17.27:8020/analyse/netflow_analyse/model")
		//		val model = PipelineModel.load("hdfs://172.17.17.27:8020/analyse/flowMoni/model")
		val model_bc = ssc.sparkContext.broadcast(model)

		val stream = KafkaUtils.createDirectStream[String, String](
			ssc,
			PreferConsistent,
			Subscribe[String, String](Array("netflow"), kafkaParams)
		)
		stream.foreachRDD(rdd => {

			val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
			//rdd.coalesce(20, false)
			try {
				val row_rdd = deal(rdd, model_bc)
				stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
			} catch {

				case e: Exception => {

					e.printStackTrace()
				}
			}

		})

		ssc.start()
		ssc.awaitTermination()
	}
	def deal(rdd: RDD[ConsumerRecord[String, String]], model_bc: Broadcast[PipelineModel]): Unit = {

		val model = model_bc.value

		//val pattern="""downpkts -> (.*?), packernum -> (.*?), srcip -> (.*?), lasttime -> (.*?), proto -> (.*?), es_type -> (.*?), recordtime -> (.*?), recordid -> (.*?), starttime -> (.*?), srcport -> (.*?), dstip -> (.*?), ups -> (.*?), proto7 -> (.*?), uppkts -> (.*?), dstport -> (.*?), downs -> (.*?)""".r

		val row_rdd = rdd.mapPartitions(iter => {
			val res_seq = scala.collection.mutable.ListBuffer.empty[Row]
			while (iter.hasNext) {
				val pattern = new Regex(
					"""downpkts -> (.*?), packernum -> (.*?), srcip -> (.*?), lasttime -> (.*?), proto -> (.*?), es_type -> (.*?), recordtime -> (.*?), recordid -> (.*?), starttime -> (.*?), srcport -> (.*?), dstip -> (.*?), ups -> (.*?), proto7 -> (.*?), uppkts -> (.*?), dstport -> (.*?), downs -> (.*?)\)""",
					"downpkts", "packernum", "srcip", "lasttime", "proto", "es_type", "recordtime", "recordid", "starttime", "srcport", "dstip", "ups", "proto7", "uppkts", "dstport", "downs"
				)
				val netflow = iter.next().value()

				pattern.findFirstMatchIn(netflow) match {
					case Some(s) => try {

						res_seq += Row(s.group("downpkts").toDouble, s.group("packernum").toDouble, s.group("srcip"),
							s.group("lasttime").toDouble, s.group("proto"), s.group("recordtime").toDouble,
							s.group("recordid"), s.group("starttime").toDouble, s.group("srcport"), s.group("dstip"),
							s.group("ups").toDouble, s.group("uppkts").toDouble, s.group("dstport"), s.group("downs").toDouble, netflow)
					} catch {
						case e: Exception => {
							var err = e.getMessage + "\n" + e.getStackTraceString + "\n---------------\n" + netflow + "\n"
							throw new Exception(err)
						}
					}
					case None => None
				}

			}

			res_seq.toIterator
		}).filter(f => { f != null })

		val schema = StructType(List(
			StructField("downpkts", DoubleType, false),
			StructField("packernum", DoubleType, false),
			StructField("srcip", StringType, false),
			StructField("lasttime", DoubleType, false),
			StructField("proto", StringType, false),
			StructField("recordtime", DoubleType, false),
			StructField("recordid", StringType, false),
			StructField("starttime", DoubleType, false),
			StructField("srcport", StringType, false),
			StructField("dstip", StringType, false),
			StructField("ups", DoubleType, false),
			StructField("uppkts", DoubleType, false),
			StructField("dstport", StringType, false),
			StructField("downs", DoubleType, false),
			StructField("netflow", StringType, false)
		))

		val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)
		import sqlContext.implicits._
		val rdata = sqlContext.createDataFrame(row_rdd, schema) //.coalesce(20)

		rdata.createOrReplaceTempView("data")

		//rdata.sqlContext.sparkSession.sql(sqlText)
		val d = sqlContext.sql(
			"""SELECT 
						|data.srcip,
						|data.dstip,
						|cast(data.recordtime as double),
						|data.proto,
		        |cast(SUM(data.lasttime-data.starttime) as double) AS spandtime,
						|cast(SUM(data.uppkts+data.downpkts) as double) AS packernum, 
						|cast(SUM(data.ups+data.downs) as double) AS bytesize,
		        |cast(COUNT(DISTINCT data.srcport) as double) AS src_flow_port_num,
		        |cast(COUNT(DISTINCT data.dstport)as double) AS dst_flow_port_num,
		        |cast(CASE WHEN data.PROTO = 'tcp' OR  data.PROTO = 'TCP' THEN 1.0 else 0.0 END as double)AS tcp,
		        |cast(CASE WHEN data.PROTO = 'udp' OR  data.PROTO = 'UDP' THEN 1.0 else 0.0 END as double)AS udp,
		        |cast(COUNT(*) as double) AS flownum,
						|CONCAT_WS(',', COLLECT_SET(DISTINCT data.netflow)) AS netflows,
		        |CONCAT_WS(',', COLLECT_SET(DISTINCT data.SRCPORT)) AS srcports,
		        |CONCAT_WS(',', COLLECT_SET(DISTINCT data.DSTPORT)) AS dstports FROM data
		        |GROUP BY data.SRCIP,data.PROTO,data.DSTIP,data.RECORDTIME
		      """.stripMargin
		)
		val predict = model.transform(d)
		dealPredict(predict)

	}

	def dealPredict(predict: DataFrame): Unit = {
		predict.foreachPartition(rdd => {
			val conn = PgSqlUtil.getPGConnection()
			val pattern = new Regex(
				"""downpkts -> (.*?), packernum -> (.*?), srcip -> (.*?), lasttime -> (.*?), proto -> (.*?), es_type -> (.*?), recordtime -> (.*?), recordid -> (.*?), starttime -> (.*?), srcport -> (.*?), dstip -> (.*?), ups -> (.*?), proto7 -> (.*?), uppkts -> (.*?), dstport -> (.*?), downs -> (.*?)\)""",
				"downpkts", "packernum", "srcip", "lasttime", "proto", "es_type", "recordtime", "recordid", "starttime", "srcport", "dstip", "ups", "proto7", "uppkts", "dstport", "downs"
			)
			val insert_data = scala.collection.mutable.ArrayBuffer.empty[Map[String,Any]]
			val name_type_mapping=Map(
					"recordid"->"string",
					"SRCIP"->  "string",
					"DSTIP"-> "string",
					"ATTACKTYPE"->"string",
					"RECORDTIME"->"time",
					"COSTTIME"->"double",
					"SRCPORT"->  "int",
					"DSTPORT"->"int",
					"PROTO"->"string",
					"PACKERNUM"->"int",
					"BYTESIZE"->"int",
					"FLOWNUMA"->"int",
					"FLOWNUMS"->"int",
					"FLOWNUMD"->"int"
			)
			rdd.foreach(row => {
				val ATTACKTYPE = row.getAs[String]("predictedLabel")
				if (ATTACKTYPE != "normal") {
					val RECORDTIME = row.getAs[Double]("recordtime").toLong * 1000
					val COSTTIME = row.getAs[Double]("spandtime")* 1000
					val PROTO = row.getAs[String]("proto")
					val PACKERNUM = row.getAs[Double]("packernum").toInt
					val BYTESIZE = row.getAs[Double]("bytesize").toInt
					val FLOWNUMA = row.getAs[Double]("flownum").toInt
					val FLOWNUMS = row.getAs[Double]("src_flow_port_num").toInt
					val FLOWNUMD = row.getAs[Double]("dst_flow_port_num").toInt
					val netflows = row.getAs[String]("netflows")

					pattern.findAllMatchIn(netflows).foreach(m => {
						val data_map=Map(
								"recordid"->("ignore"+m.group("recordid")),
								"SRCIP"->  m.group("srcip"),
								"DSTIP"->  m.group("dstip"),
								"ATTACKTYPE"->ATTACKTYPE,
								"RECORDTIME"->RECORDTIME,
								"COSTTIME"->COSTTIME,
								"SRCPORT"->  m.group("srcport").toInt,
								"DSTPORT"->m.group("dstport").toInt,
								"PROTO"->PROTO,
								"PACKERNUM"->PACKERNUM,
								"BYTESIZE"->BYTESIZE,
								"FLOWNUMA"->FLOWNUMA,
								"FLOWNUMS"->FLOWNUMS,
								"FLOWNUMD"->FLOWNUMD
						
						)
						insert_data+=data_map
//						PgSqlUtil.insertBatch(conn, "t_siem_netflow_rfpredict",name_type_mapping, Array(data_map))
					})
				}

			})
			PgSqlUtil.insertBatch(conn, "t_siem_netflow_rfpredict",name_type_mapping, insert_data.toArray)
			conn.close()
		})

	}
	def dealPredict1(predict: DataFrame): Unit = {
		predict.foreachPartition(rdd => {
			val conn = PgSqlUtil.getPGConnection()
			rdd.foreach(row => {
				val ATTACKTYPE = row.getAs[String]("predictedLabel")
				if (ATTACKTYPE != "normal") {
					val SRCIP = row.getAs[String]("srcip")
					val DSTIP = row.getAs[String]("dstip")

					val RECORDTIME = new Timestamp(row.getAs[Double]("recordtime").toLong * 1000)
					val COSTTIME = row.getAs[Double]("spandtime")
					val SRCPORTs = row.getAs[String]("srcports").split(",")
					val DSTPORTs = row.getAs[String]("dstports").split(",")
					val PROTO = row.getAs[String]("proto")
					val PACKERNUM = row.getAs[Double]("packernum").toInt
					val BYTESIZE = row.getAs[Double]("bytesize").toInt
					val FLOWNUMA = row.getAs[Double]("flownum").toInt
					val FLOWNUMS = row.getAs[Double]("src_flow_port_num").toInt
					val FLOWNUMD = row.getAs[Double]("dst_flow_port_num").toInt
					for (SRCPORT <- SRCPORTs; DSTPORT <- DSTPORTs) {
						//val prep = conn.prepareStatement("""INSERT INTO public.t_siem_netflow_rfpredict (SRCIP,DSTIP,ATTACKTYPE,RECORDTIME,COSTTIME,SRCPORT,DSTPORT,PROTO,PACKERNUM,BYTESIZE,FLOWNUMA,FLOWNUMS,FLOWNUMD)
						//																							                               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?); """)

						val prep = conn.prepareStatement("""INSERT INTO public.t_siem_netflow_rfpredict  
																															                               VALUES (?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?); """)
						prep.setString(1, "ignore")
						prep.setString(2, SRCIP)
						prep.setString(3, DSTIP)
						prep.setString(4, ATTACKTYPE)
						prep.setTimestamp(5, RECORDTIME)
						prep.setDouble(6, COSTTIME)
						prep.setObject(7, SRCPORT.toDouble.toInt)
						prep.setObject(8, DSTPORT.toDouble.toInt)
						prep.setString(9, PROTO)
						prep.setObject(10, PACKERNUM)
						prep.setObject(11, BYTESIZE)
						prep.setObject(12, FLOWNUMA)
						prep.setObject(13, FLOWNUMS)
						prep.setObject(14, FLOWNUMD)
						prep.executeUpdate
					}
				}

			})
			conn.close()
		})
	}

}
/** Lazily instantiated singleton instance of SQLContext */
object SQLContextSingleton {

	@transient private var instance: SQLContext = _

	def getInstance(sparkContext: SparkContext): SQLContext = {
		if (instance == null) {
			instance = new SQLContext(sparkContext)
		}
		instance
	}
}