package com.nfjd.analyse.flowMoni

import org.apache.spark.ml.PipelineModel
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import java.sql.DriverManager
import java.sql.Timestamp
import org.apache.spark.storage.StorageLevel
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import com.nfjd.util.PgSqlUtil

//com.nfjd.analyse.flowMoni.PredictOffline
object PredictOffline {
	def main(args: Array[String]): Unit = {
		val conf = new SparkConf().setAppName("MoniFlowAnalyse_predict") //.setMaster("local[2]")
		conf.set("es.nodes", "172.17.17.30,172.17.17.31,172.17.17.32").set("es.port", "9300")
		conf.set("spark.yarn.executor.memoryOverhead", "4024")
		val sc = new SparkContext(conf)

		val sqlContext = new SQLContext(sc)

		val d = DataPretreatment.getESPosSamBeforeTime(sqlContext, 2)
		d.write.parquet("hdfs://172.17.17.24:8020/analyse/flowMoni/testData")
//		d.persist(StorageLevel.MEMORY_AND_DISK)
//		val model = PipelineModel.load("hdfs://172.17.17.24:8020/analyse/flowMoni/model")
//		val predict = model.transform(d)
//
//		predict.foreachPartition(rdd_i => {
//			println("---------")
//			
//			val conn = PgSqlUtil.getPGConnection()
//			rdd_i.foreach(row => {
//				val id = "ignore"
//				val date = new Timestamp(row.getAs[Long]("date") * 1000)
//				val srcip = row.getAs[String]("srcip")
//				val dstip = row.getAs[String]("dst")
//				val uri = row.getAs[String]("uri")
//				val predictionnb = row.getAs[Double]("predictionRF")
//				val probabilitynb = row.getAs[Double]("probabilityRF")
//				val predictionlr = row.getAs[Double]("predictionLR")
//				val probabilitylr = row.getAs[Double]("probabilityLR")
//
//				val prep = conn.prepareStatement(
//					"""INSERT INTO public.t_moni_flow_predict(id,date,srcip,dstip,uri,predictionnb,probabilitynb,predictionlr,probabilitylr,url)  
//																												                               VALUES (?,?, ?, ?, ?, ?, ?, ?, ?, ?); """
//				)
//				prep.setString(1, id)
//				prep.setTimestamp(2, date)
//				prep.setString(3, srcip)
//				prep.setString(4, dstip)
//				prep.setString(5, uri)
//				prep.setDouble(6, predictionnb)
//				prep.setDouble(7, probabilitynb)
//				prep.setDouble(8, predictionlr)
//				prep.setDouble(9, probabilitylr)
//				prep.setString(10, uri)
//				prep.execute()
//
//			})
//			conn.close()
//		})
	}

}