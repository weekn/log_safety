package com.nfjd.analyse.flowMoni
import scala.util.matching.Regex
import org.apache.spark.ml.linalg.DenseVector
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
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame
import java.sql.Timestamp
import org.apache.spark.sql.types.LongType

//com.nfjd.analyse.flowMoni.PredictOnline
object PredictOnline {
	def main(args: Array[String]): Unit = {
		val configUtil = new ConfigUtil()
		configUtil.readConfig()
		val conf = new SparkConf().setAppName("flow_sql_predict_ss") //.setMaster("local[2]")
		conf.set("es.nodes", configUtil.es_serves).set("es.port", "9300")
		conf.set("es.index.auto.create", "true")
		//conf.set("es.batch.size.entries", "1")
		conf.set("spark.streaming.stopGracefullyOnShutdown", "true")
		conf.set("spark.streaming.kafka.maxRatePerPartition", "5000") //限制每秒每个消费线程读取每个kafka分区最大的数据量
		conf.set("spark.streaming.receiver.maxRate", "5000") //设置每次接收的最大数量
		

		val ssc = new StreamingContext(conf, Seconds(10))

		val kafkaParams = Map[String, Object](
			"bootstrap.servers" -> configUtil.kafka_spark_servers, //"172.17.17.21:9092,172.17.17.22:9092",
			"key.deserializer" -> classOf[StringDeserializer],
			"value.deserializer" -> classOf[StringDeserializer],
			"group.id" -> "flow_sql_predict_01",
			"auto.offset.reset" -> "latest",
			"enable.auto.commit" -> (false: java.lang.Boolean)
		)
		val model = PipelineModel.load("hdfs://172.17.17.27:8020/analyse/flowMoni/model")
		val model_bc = ssc.sparkContext.broadcast(model)

		val stream = KafkaUtils.createDirectStream[String, String](
			ssc,
			PreferConsistent,
			Subscribe[String, String](Array("flow"), kafkaParams)
		)
		stream.foreachRDD(rdd=>{
			
			val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
			try {
				val row_rdd=deal(rdd,model_bc)
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
	def deal(rdd: RDD[ConsumerRecord[String, String]],model_bc:Broadcast[PipelineModel]):Unit={
		val sqlContext = SQLContext.getOrCreate(rdd.sparkContext)
		val model=model_bc.value
  	import sqlContext.implicits._
		val row_rdd=rdd.mapPartitions(iter=>{
			val res_seq = scala.collection.mutable.ListBuffer.empty[Row]
			val dtreater=new DataPretreater()
			val pattern="""id\=(.*),uri=.*\?(.*),dstip=(.*),srcip=(.*),date=(.*)""".r
			val pattern_uri="""uri=(.*?),""".r
			while (iter.hasNext){
				val flow = iter.next().value()
				pattern.findFirstMatchIn(flow) match{
					case Some(s)=>{
						val id=s.group(1)
						val uri=s.group(2)
						val dstip=s.group(3)
						val srcip=s.group(4)
						val date=s.group(5).toLong
						val uri_ori=pattern_uri.findFirstMatchIn(flow).get.group(1)
						val rcode_uri = dtreater.preDealStr(uri)
//						var r_seq = seq.updated(uri_index, rcode)
//	
//						Row.fromSeq(r_seq)
						res_seq+=Row(id,rcode_uri, uri_ori, date,srcip,dstip)
						
					}
					case None=>null
				}
				
			}
			
			res_seq.toIterator
		}).filter(f=>f!=null)
		val schema = StructType(List(
				StructField("id", StringType),
				StructField("uri", StringType),
				StructField("uri_ori", StringType),
				StructField("date", LongType),
				StructField("srcip", StringType),
				StructField("dstip", StringType)
	    ))
		val rdata =sqlContext.createDataFrame(row_rdd, schema)
		val predict=model.transform(rdata)
		dealPredict(predict)
		
	}
	def dealPredict(predict:DataFrame):Unit={
			predict.foreachPartition(rdd_i => {
			

			val conn = PgSqlUtil.getPGConnection()
			rdd_i.foreach(row => {
				val id = "ignore" +  row.getAs[String]("id")
				val date = new Timestamp(row.getAs[Long]("date") * 1000)
				val srcip = row.getAs[String]("srcip")
				val dstip = row.getAs[String]("dstip")
				val uri = row.getAs[String]("uri_ori")
				val predictionnb = row.getAs[Double]("predictionRF")
				val probabilitynb = row.getAs[DenseVector]("probabilityRF").apply(predictionnb.toInt)
				val predictedLabelnb=row.getAs[String]("predictedLabelRF")
				
				val predictionlr = row.getAs[Double]("predictionLR")
				val probabilitylr = row.getAs[DenseVector]("probabilityLR").apply(predictionlr.toInt)
				val predictedLabellr=row.getAs[String]("predictedLabelLR")
				
				if (predictedLabelnb == "sql" || predictedLabellr == "sql" ) {
					val prep = conn.prepareStatement(
						"""INSERT INTO public.t_moni_flow_predict(id,date,srcip,dstip,uri,predictionnb,probabilitynb,predictionlr,probabilitylr,url)  
																												                               VALUES (?,?, ?, ?, ?, ?, ?, ?, ?, ?); """
					)
					prep.setString(1, id)
					prep.setTimestamp(2, date)
					prep.setString(3, srcip)
					prep.setString(4, dstip)
					prep.setString(5, uri)
					prep.setDouble(6, predictionnb)
					prep.setDouble(7, probabilitynb)
					prep.setDouble(8, predictionlr)
					prep.setDouble(9, probabilitylr)
					prep.setString(10, uri)
					prep.execute()
				}

			})
			conn.close()
		})
	}
	
}