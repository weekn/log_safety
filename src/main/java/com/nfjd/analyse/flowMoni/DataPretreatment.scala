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
	val structFields = List(StructField("label", StringType), StructField("uri", StringType))
	val types = StructType(structFields)
	def main(args: Array[String]): Unit = {
		
	}
	
	

	def getESPosSamBeforeTime(sqlContext: SQLContext,day:Int):DataFrame={
		val df = sqlContext.read.format("org.elasticsearch.spark.sql").load("flow/flow")
		val current=TimeUtil.getCurrentTimeStamp()
		val ts=current-day*86400

		val data = df.filter(df("date").gt(ts).and(df("date").lt(current))).select(df.col("uri"),df.col("date"),df.col("srcip"),df.col("dstip"))
		data.write.parquet("hdfs://172.17.17.24:8020/analyse/flowMoni/testData1")
		val r=data.rdd.map(line=>{
			val pattern="""\?(.*)""".r
			val res=pattern.findFirstIn(line.getAs[String]("uri"))
			res match{
				case Some(s)=>{
					Row("sql",URLDecoder.decode(s.replaceAll("%(?![0-9a-fA-F]{2})", "%25")).toLowerCase())
				}
				case None=>{
					Row("sql","go")
				}
			}
			
		})
		
		val rdata = sqlContext.createDataFrame(r, types)
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