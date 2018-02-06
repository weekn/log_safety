package com.nfjd.analyse.flowMoni
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import com.nfjd.util.TimeUtil
import org.apache.spark.sql.types.DoubleType
import java.net.URLDecoder
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import org.apache.spark.SparkContext


//com.nfjd.analyse.netflowAnalyse.DataPretreatment
object DataPretreatment {
	
	def main(args: Array[String]): Unit = {
		
	}
	
	

	def getESPosSamBeforeTime(sqlContext: SQLContext,day:Int):DataFrame={
		val df = sqlContext.read.format("org.elasticsearch.spark.sql").load("flow/flow")
		val current=TimeUtil.getCurrentTimeStamp()
		val ts=current-day*86400

		val data = df.filter(df("date").gt(ts).and(df("date").lt(current))).select(df.col("uri"),df.col("uri").as("uri_ori"),df.col("date"),df.col("srcip"),df.col("dstip"))
		val sch=data.schema
		//data.write.parquet("hdfs://172.17.17.24:8020/analyse/flowMoni/testData1")
		val r=data.rdd.map(line=>{
			
			var seq=line.toSeq
			val uri_index=line.fieldIndex("uri")
			val pattern="""\?(.*)""".r
			val res=pattern.findFirstIn(line.getAs[String]("uri"))
			res match{
				case Some(s)=>{
					
					val rcode=URLDecoder.decode(s.replaceAll("%(?![0-9a-fA-F]{2})", "%25")).toLowerCase()
					var r_seq=seq.updated(uri_index, rcode)
				
					Row.fromSeq(r_seq)
				}
				case None=>{
//					val r_seq=seq.updated(uri_index, "eeeeeeeeeeeee")
//					Row.fromSeq(r_seq)
					null
				}
			}
			
		}).filter(f=>f!=null)
		
		val rdata = sqlContext.createDataFrame(r, sch)
		rdata
		
	}
  def dealCsvData(spark: SparkSession,path:String): DataFrame = {
		val data = spark.sqlContext.read.format("com.databricks.spark.csv")
			.option("header", "true")
			.option("inferSchema", "true") 
			.load(path) 
		val broaddata = spark.sparkContext.broadcast(data)
		broaddata.value.createOrReplaceTempView("data")
		val d = spark.sql(
			"""SELECT data.srcip,data.dstip,data.recordtime,data.proto,data.label as label,
        |SUM(data.costtime) AS spandtime,SUM(data.packernum) AS packernum, SUM(data.bytesize) AS bytesize,
        |COUNT(DISTINCT data.srcport) AS src_flow_port_num,
        |COUNT(DISTINCT data.dstport) AS dst_flow_port_num,
        |CASE WHEN data.PROTO = 'tcp' OR  data.PROTO = 'TCP' THEN 1 else 0 END AS tcp,
        |CASE WHEN data.PROTO = 'udp' OR  data.PROTO = 'UDP' THEN 1 else 0 END AS udp,
        |COUNT(*) AS flownum,
        |CONCAT_WS(',', COLLECT_SET(DISTINCT data.SRCPORT)) AS srcports,
        |CONCAT_WS(',', COLLECT_SET(DISTINCT data.DSTPORT)) AS dstports FROM data
        |GROUP BY data.SRCIP,data.PROTO,data.DSTIP,data.RECORDTIME,data.label
      """.stripMargin
		)
		d
	}
  def dealCsvDataWithLabel(spark: SparkSession,path:String,label:String): DataFrame = {
		val data = spark.sqlContext.read.format("com.databricks.spark.csv")
			.option("header", "true")
			.option("inferSchema", "true") 
			.load(path) 
		val broaddata = spark.sparkContext.broadcast(data)
		broaddata.value.createOrReplaceTempView("data")
		val d = spark.sql(
			"""SELECT data.srcip,data.dstip,data.recordtime,data.proto,'"""+label+"""' as label,
        |SUM(data.costtime) AS spandtime,SUM(data.packernum) AS packernum, SUM(data.bytesize) AS bytesize,
        |COUNT(DISTINCT data.srcport) AS src_flow_port_num,
        |COUNT(DISTINCT data.dstport) AS dst_flow_port_num,
        |CASE WHEN data.PROTO = 'tcp' OR  data.PROTO = 'TCP' THEN 1 else 0 END AS tcp,
        |CASE WHEN data.PROTO = 'udp' OR  data.PROTO = 'UDP' THEN 1 else 0 END AS udp,
        |COUNT(*) AS flownum,
        |CONCAT_WS(',', COLLECT_SET(DISTINCT data.SRCPORT)) AS srcports,
        |CONCAT_WS(',', COLLECT_SET(DISTINCT data.DSTPORT)) AS dstports FROM data
        |GROUP BY data.SRCIP,data.PROTO,data.DSTIP,data.RECORDTIME
      """.stripMargin
		)
		d
	}
}