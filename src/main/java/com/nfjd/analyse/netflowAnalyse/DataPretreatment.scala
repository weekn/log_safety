package com.nfjd.analyse.netflowAnalyse
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import com.nfjd.util.TimeUtil
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.storage.StorageLevel


//com.nfjd.analyse.netflowAnalyse.DataPretreatment
object DataPretreatment {
	def main(args: Array[String]): Unit = {
		val conf = new SparkConf().setAppName("netflowAnalyse.DataPretreatment")//.setMaster("local[2]")
		conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
		conf.set("spark.yarn.executor.memoryOverhead","5G")
		conf.set("spark.network.timeout","600")
		conf.set("es.nodes", "172.17.17.30").set("es.port", "9300")
		val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
		
		val data = spark.sqlContext.read.format("org.elasticsearch.spark.sql").load("netflow/netflow").limit(99999999)
//		val a=new DataPretreatment().dealCsvData(spark, "D:/work/eclipse-oxgen/spark/test2.csv")
//		a.show()
	  data.repartition(300)
		val broaddata = spark.sparkContext.broadcast(data)
		broaddata.value.createOrReplaceTempView("data")
		val es_d = spark.sql(
			"""SELECT data.srcip,data.dstip,data.recordtime,data.proto,'normal' as label,
        |SUM(data.lasttime-data.starttime) AS spandtime,SUM(data.uppkts+data.downpkts) AS packernum, 
				|SUM(data.ups+data.downs) AS bytesize,
        |COUNT(DISTINCT data.srcport) AS src_flow_port_num,
        |COUNT(DISTINCT data.dstport) AS dst_flow_port_num,
        |CASE WHEN data.PROTO = 'tcp' OR  data.PROTO = 'TCP' THEN 1.0 else 0.0 END AS tcp,
        |CASE WHEN data.PROTO = 'udp' OR  data.PROTO = 'UDP' THEN 1.0 else 0.0 END AS udp,
        |COUNT(*) AS flownum,
        |CONCAT_WS(',', COLLECT_SET(DISTINCT data.SRCPORT)) AS srcports,
        |CONCAT_WS(',', COLLECT_SET(DISTINCT data.DSTPORT)) AS dstports FROM data 
        |GROUP BY data.SRCIP,data.PROTO,data.DSTIP,data.RECORDTIME
      """.stripMargin
		)
		es_d.write.mode("append").parquet("hdfs://172.17.17.27:8020/analyse/netflow_analyse/pos_sam_df")
//	  val dos=dealCsvDataWithLabel(spark," ","DOS")
//	  val syn=dealCsvDataWithLabel(spark,"hdfs://172.17.17.24:8020/analyse/netflow_analyse/negsample/syn.csv","FLOOD")
//	  val udp=dealCsvDataWithLabel(spark,"hdfs://172.17.17.24:8020/analyse/netflow_analyse/negsample/udp.csv","FLOOD")
//	  val port=dealCsvDataWithLabel(spark,"hdfs://172.17.17.24:8020/analyse/netflow_analyse/negsample/portscan.csv","PORTSCAN")
//	  val flood=syn.union(udp)
//	  
//	  dos.write.mode("append").parquet("hdfs://172.17.17.24:8020/analyse/netflow_analyse/neg_dos")
//	  port.write.mode("append").parquet("hdfs://172.17.17.24:8020/analyse/netflow_analyse/neg_port")
//	  flood.write.mode("append").parquet("hdfs://172.17.17.24:8020/analyse/netflow_analyse/neg_flood")
//	  val dos_num=dos.count()
//	  val flood_num=syn.count()+udp.count()
//	  val port_num=port.count()
//	  val max_n=List(dos_num,flood_num,port_num).max
//	  
//	  val dos_t=max_n/dos_num
//	  val flood_t=max_n/flood_num
//	  val port_t=max_n/flood_num
//	  
//	  var r_d=flood
//	  for (i<-0 until dos_t.toInt){
//	  	r_d=r_d.union(dos)
//	  }
//		
//		for (i<-0 until port_t.toInt){
//	  	r_d=r_d.union(port)
//	  }
//		
//		for (i<-1 until flood_t.toInt){
//	  	r_d=r_d.union(flood)
//	  }
//	  r_d.persist(StorageLevel.MEMORY_AND_DISK)
//	  r_d.write.mode("append").parquet("hdfs://172.17.17.24:8020/analyse/netflow_analyse/neg_sam_df")
	}
	def getData(spark: SparkSession):DataFrame={
		//val neg_data=spark.sqlContext.read.parquet("hdfs://172.17.17.24:8020/analyse/netflow_analyse/neg_sam_df")
		val dos=dealCsvDataWithLabel(spark,"hdfs://172.17.17.24:8020/analyse/netflow_analyse/negsample/cc.csv","DOS")
	  val syn=dealCsvDataWithLabel(spark,"hdfs://172.17.17.24:8020/analyse/netflow_analyse/negsample/syn.csv","FLOOD")
	  val udp=dealCsvDataWithLabel(spark,"hdfs://172.17.17.24:8020/analyse/netflow_analyse/negsample/udp.csv","FLOOD")
	  val port=dealCsvDataWithLabel(spark,"hdfs://172.17.17.24:8020/analyse/netflow_analyse/negsample/portscan.csv","PORTSCAN")
	  val flood=syn.union(udp)
	  
	  val dos_num=dos.count()
	  val flood_num=syn.count()+udp.count()
	  val port_num=port.count()
	  val max_n=List(dos_num,flood_num,port_num).max
	  
	  val dos_t=max_n/dos_num
	  val flood_t=max_n/flood_num
	  val port_t=max_n/flood_num
	  
	  var r_d=flood
	  for (i<-0 until dos_t.toInt){
	  	r_d=r_d.union(dos)
	  }
		
		for (i<-0 until port_t.toInt){
	  	r_d=r_d.union(port)
	  }
		
		for (i<-1 until flood_t.toInt){
	  	r_d=r_d.union(flood)
	  }
	  r_d.persist(StorageLevel.DISK_ONLY)
		val neg_num=r_d.count()
		val pos_data=getESPosSam(spark,neg_num*5)
		pos_data.persist(StorageLevel.DISK_ONLY)
		
		pos_data.union(r_d)
	}
	
	
	def getESPosSam(spark: SparkSession,num:Long):DataFrame={
		val data = spark.sqlContext.read.format("org.elasticsearch.spark.sql").load("netflow/netflow")
//		val a=new DataPretreatment().dealCsvData(spark, "D:/work/eclipse-oxgen/spark/test2.csv")
//		a.show()
	  data.limit(num.toInt*20).repartition(50)
		val broaddata = spark.sparkContext.broadcast(data)
		broaddata.value.createOrReplaceTempView("data")
		val es_d = spark.sql(
			"""SELECT data.srcip,data.dstip,data.recordtime,data.proto,'normal' as label,
        |SUM(data.lasttime-data.starttime) AS spandtime,SUM(data.uppkts+data.downpkts) AS packernum, 
				|SUM(data.ups+data.downs) AS bytesize,
        |COUNT(DISTINCT data.srcport) AS src_flow_port_num,
        |COUNT(DISTINCT data.dstport) AS dst_flow_port_num,
        |CASE WHEN data.PROTO = 'tcp' OR  data.PROTO = 'TCP' THEN 1.0 else 0.0 END AS tcp,
        |CASE WHEN data.PROTO = 'udp' OR  data.PROTO = 'UDP' THEN 1.0 else 0.0 END AS udp,
        |COUNT(*) AS flownum,
        |CONCAT_WS(',', COLLECT_SET(DISTINCT data.SRCPORT)) AS srcports,
        |CONCAT_WS(',', COLLECT_SET(DISTINCT data.DSTPORT)) AS dstports FROM data 
        |GROUP BY data.SRCIP,data.PROTO,data.DSTIP,data.RECORDTIME
      """.stripMargin
		).limit(num.toInt)
		es_d
	}
	def getESPosSamBeforeTime(spark: SparkSession,day:Int):DataFrame={
		val df = spark.sqlContext.read.format("org.elasticsearch.spark.sql").load("netflow/netflow")
		val current=TimeUtil.getCurrentTimeStamp()
		val ts=current-day*86400
//		val a=new DataPretreatment().dealCsvData(spark, "D:/work/eclipse-oxgen/spark/test2.csv")
//		a.show()
		df.printSchema()
		
		val data = df.filter(df("recordtime").gt(ts))
		val broaddata = spark.sparkContext.broadcast(data)
		broaddata.value.createOrReplaceTempView("data")
		val d = spark.sql(
			"""SELECT data.srcip,data.dstip,data.recordtime,data.proto,'normal' as label,
        |SUM(data.lasttime-data.starttime) AS spandtime,SUM(data.uppkts+data.downpkts) AS packernum, 
				|SUM(data.ups+data.downs) AS bytesize,
        |COUNT(DISTINCT data.srcport) AS src_flow_port_num,
        |COUNT(DISTINCT data.dstport) AS dst_flow_port_num,
        |CASE WHEN data.PROTO = 'tcp' OR  data.PROTO = 'TCP' THEN 1.0 else 0.0 END AS tcp,
        |CASE WHEN data.PROTO = 'udp' OR  data.PROTO = 'UDP' THEN 1.0 else 0.0 END AS udp,
        |COUNT(*) AS flownum,
        |CONCAT_WS(',', COLLECT_SET(DISTINCT data.SRCPORT)) AS srcports,
        |CONCAT_WS(',', COLLECT_SET(DISTINCT data.DSTPORT)) AS dstports FROM data
        |GROUP BY data.SRCIP,data.PROTO,data.DSTIP,data.RECORDTIME
      """.stripMargin
		)
		val data_double = d.select(d.col("srcip"), d.col("dstip"), d.col("recordtime"), d.col("proto"), d.col("srcports"), d.col("dstports"),
				d.col("spandtime").cast(DoubleType).as("spandtime"),
			d.col("packernum").cast(DoubleType).as("packernum"),
			d.col("bytesize").cast(DoubleType).as("bytesize"),
			d.col("src_flow_port_num").cast(DoubleType).as("src_flow_port_num"),
			d.col("dst_flow_port_num").cast(DoubleType).as("dst_flow_port_num"),
			d.col("tcp").cast(DoubleType).as("tcp"),
			d.col("udp").cast(DoubleType).as("udp"),
			d.col("flownum").cast(DoubleType).as("flownum"),
			d.col("label")
		)
		data_double
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
		val data = spark.sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load(path) 
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