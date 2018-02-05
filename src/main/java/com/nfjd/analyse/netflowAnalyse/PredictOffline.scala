package com.nfjd.analyse.netflowAnalyse

import org.apache.spark.ml.PipelineModel
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import java.sql.DriverManager
import java.sql.Timestamp
import org.apache.spark.storage.StorageLevel

//com.nfjd.analyse.netflowAnalyse.PredictOffline
object PredictOffline {
	def main(args: Array[String]): Unit = {
		val conf = new SparkConf().setAppName("PredictOffline")
		conf.set("es.nodes", "172.17.17.30").set("es.port", "9300")
		conf.set("es.index.auto.create", "true")
		conf.set("spark.yarn.executor.memoryOverhead", "4024")
		val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
		//val predict = spark.sqlContext.read.parquet("hdfs://172.17.17.24:8020/analyse/netflow_analyse/tmp_predict_res")
		val d=DataPretreatment.getESPosSamBeforeTime(spark,2)
		d.persist(StorageLevel.MEMORY_AND_DISK)
		val model=PipelineModel.load("hdfs://172.17.17.24:8020/analyse/netflow_analyse/model")
		val predict=model.transform(d)
		val bdddd = spark.sparkContext.broadcast(predict)
		bdddd.value.createOrReplaceTempView("data")
		val ddd = spark.sql(
			"""SELECT data.srcip,data.dstip,data.predictedLabel,data.recordtime,data.spandtime,
				|data.srcports,data.dstports,data.proto,data.packernum,data.bytesize,data.flownum,data.src_flow_port_num,data.dst_flow_port_num
				|from data where data.predictedLabel != 'normal'
				""".stripMargin
		)
		
		ddd.foreachPartition(rdd_i => {
			println("---------")
			val conn_str = "jdbc:postgresql://172.17.17.70:5432/nxsoc5"
			val conn = DriverManager.getConnection(conn_str, "postgres", "postgres")
			rdd_i.foreach(row => {
				println(row.apply(0), row.apply(1), row.apply(2))
				val SRCIP = row.apply(0).toString()
				val DSTIP = row.apply(1).toString()
				val ATTACKTYPE = row.apply(2).toString()
				val RECORDTIME = new Timestamp(row.apply(3).toString().toLong*1000)
				val COSTTIME = row.apply(4).toString().toDouble
				val SRCPORTs = row.apply(5).toString().split(",")
				val DSTPORTs = row.apply(6).toString().split(",")
				val PROTO = row.apply(7).toString()
				val PACKERNUM = row.apply(8).toString().toDouble.toInt
				val BYTESIZE = row.apply(9).toString().toDouble.toInt
				val FLOWNUMA = row.apply(10).toString().toDouble.toInt
				val FLOWNUMS = row.apply(11).toString().toDouble.toInt
				val FLOWNUMD = row.apply(12).toString().toDouble.toInt
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

			})
			conn.close()
		})
	}
	def go(args: Array[String]): Unit = {
		val conf = new SparkConf().setAppName("PredictOffline") //.setMaster("local[2]")
		conf.set("es.nodes", "172.17.17.30").set("es.port", "9300")
		//conf.set("es.nodes", "192.168.181.234").set("es.port", "9200")
		conf.set("es.index.auto.create", "true")
		conf.set("spark.yarn.executor.memoryOverhead", "4024")

		val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
		//		val df = spark.sqlContext.read.format("org.elasticsearch.spark.sql").load("netflow/netflow")
		//		df.printSchema()

		val data = DataPretreatment.getESPosSamBeforeTime(spark, args(0).toInt)

		val model = PipelineModel.load("hdfs://172.17.17.24:8020/analyse/netflow_analyse/model")
		val predict = model.transform(data)
		// predict.write.parquet("hdfs://172.17.17.24:8020/analyse/netflow_analyse/tmp_predict_res")

		val bdddd = spark.sparkContext.broadcast(predict)
		bdddd.value.createOrReplaceTempView("data")
		val ddd = spark.sql(
			"""SELECT data.srcip,data.dstip,data.predictedLabel,data.recordtime,data.spandtime,
|data.srcports,data.dstports,data.proto,data.packernum,data.bytesize,data.flownum,data.src_flow_port_num,data.dst_flow_port_num
|from data where data.predictedLabel != 'normal'
""".stripMargin
		)

		//ddd.write.parquet("hdfs://172.17.17.24:8020/analyse/netflow_analyse/tmp_res")
		ddd.foreachPartition(rdd_i => {
			println("---------")
			val conn_str = "jdbc:postgresql://172.17.17.70:5432/nxsoc5"
			val conn = DriverManager.getConnection(conn_str, "postgres", "postgres")
			rdd_i.foreach(row => {
				println(row.apply(0), row.apply(1), row.apply(2))
				val SRCIP = row.apply(0).toString()
				val DSTIP = row.apply(1).toString()
				val ATTACKTYPE = row.apply(2).toString()
				val RECORDTIME = row.apply(3).toString()
				val COSTTIME = row.apply(4).toString()
				val SRCPORTs = row.apply(5).toString().split(",")
				val DSTPORTs = row.apply(6).toString().split(",")
				val PROTO = row.apply(7).toString()
				val PACKERNUM = row.apply(8).toString()
				val BYTESIZE = row.apply(9).toString()
				val FLOWNUMA = row.apply(10).toString()
				val FLOWNUMS = row.apply(11).toString()
				val FLOWNUMD = row.apply(12).toString()
				for (SRCPORT <- SRCPORTs; DSTPORT <- DSTPORTs) {
					//val prep = conn.prepareStatement("""INSERT INTO public.t_siem_netflow_rfpredict (SRCIP,DSTIP,ATTACKTYPE,RECORDTIME,COSTTIME,SRCPORT,DSTPORT,PROTO,PACKERNUM,BYTESIZE,FLOWNUMA,FLOWNUMS,FLOWNUMD)
					//																							                               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?); """)

					val prep = conn.prepareStatement("""INSERT INTO public.t_siem_netflow_rfpredict  
																												                               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?); """)

					prep.setString(1, SRCIP)
					prep.setString(2, DSTIP)
					prep.setString(3, ATTACKTYPE)
					prep.setString(4, RECORDTIME)
					prep.setString(5, COSTTIME)
					prep.setString(6, SRCPORT)
					prep.setString(7, DSTPORT)
					prep.setString(8, PROTO)
					prep.setString(9, PACKERNUM)
					prep.setString(10, BYTESIZE)
					prep.setString(11, FLOWNUMA)
					prep.setString(12, FLOWNUMS)
					prep.setString(13, FLOWNUMD)
					prep.executeUpdate
				}

			})
			conn.close()
		})
	}
}