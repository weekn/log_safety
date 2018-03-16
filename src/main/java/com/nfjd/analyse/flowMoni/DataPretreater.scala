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
import org.ansj.splitWord.analysis.DicAnalysis
import java.util.Properties

//com.nfjd.analyse.netflowAnalyse.DataPretreatment
class DataPretreater(sqlContext: SQLContext) extends java.io.Serializable{
	def this() {
    this(null)//这个必须要
  }
  def preDealStr(str: String): String = {
		val step1 = URLDecoder.decode(str.replaceAll("%(?![0-9a-fA-F]{2})", "%25")).toLowerCase()
		val step2 = step1.replaceAll("""[a-z]+\d+([a-z])*""", "luancode")
			.replaceAll("""\d+[a-z]+\d*""", "luancode")
			.replaceAll("""(luancode[a-z\d]*)+""", "luancode")
			.replaceAll("""\d+""", "luannumber")
		DicAnalysis.parse(step2).toStringWithOutNature("|")
	}
	def getTrainData(): DataFrame = {
		val connectionProperties = new Properties()
		
		//增加数据库的用户名(user)密码(password),指定postgresql驱动(driver)
		connectionProperties.put("user", "postgres")
		connectionProperties.put("password", "postgres")
		connectionProperties.put("driver", "org.postgresql.Driver")
		val df = this.sqlContext.read.jdbc("jdbc:postgresql://172.17.17.70:5432/nxsoc5", "traindata_flow_moni", connectionProperties)
		df.show
		val trainrdd = df.rdd.map(row =>{
			val label=row.getAs[String]("label")
			val uri=row.getAs[String]("uri")
			Row(label,preDealStr(uri))
		})
		sqlContext.createDataFrame(trainrdd, df.schema)
		
		
	}
	def getESPosSamBeforeTime(day: Int): DataFrame = {
		val df = sqlContext.read.format("org.elasticsearch.spark.sql").load("flow/flow")
		val current = TimeUtil.getCurrentTimeStamp()
		val ts = current - day * 86400

		val data = df.filter(df("date").gt(ts).and(df("date").lt(current))).select(df.col("uri"), df.col("uri").as("uri_ori"), df.col("date"), df.col("srcip"), df.col("dstip"))
		val sch = data.schema
		//data.write.parquet("hdfs://172.17.17.24:8020/analyse/flowMoni/testData1")
		val r = data.rdd.map(line => {

			var seq = line.toSeq
			val uri_index = line.fieldIndex("uri")
			val pattern = """\?(.*)""".r
			val res = pattern.findFirstIn(line.getAs[String]("uri"))
			res match {
				case Some(s) => {

					val rcode = preDealStr(s)
					var r_seq = seq.updated(uri_index, rcode)

					Row.fromSeq(r_seq)
				}
				case None => {
					//					val r_seq=seq.updated(uri_index, "eeeeeeeeeeeee")
					//					Row.fromSeq(r_seq)
					null
				}
			}

		}).filter(f => f != null)

		val rdata = sqlContext.createDataFrame(r, sch)
		rdata

	}

}