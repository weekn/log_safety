package com.nfjd.etl

import com.nfjd.model.RegPattern
import scala.util.matching.Regex
import java.util.UUID
import com.nfjd.util.TimeUtil
import java.util.Date
import com.nfjd.util.ConfigUtil
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.LongType
import com.nfjd.util.PgSqlUtil
import java.sql.Timestamp
import org.apache.spark.sql.Dataset
import java.util.Properties

object TextQuick {

	def main(args: Array[String]): Unit = {
		val conf = new SparkConf().setAppName("Word2Vec example").setMaster("local[2]")
		val sc = new SparkContext(conf)
		val sqlContext = new SQLContext(sc)
		val connectionProperties = new Properties();

		//增加数据库的用户名(user)密码(password),指定postgresql驱动(driver)
		connectionProperties.put("user", "postgres");
		connectionProperties.put("password", "postgres");
		connectionProperties.put("driver", "org.postgresql.Driver");
		val jdbcDF = sqlContext.read
			.jdbc("jdbc:postgresql://172.17.17.70:5432/nxsoc5", "t_moni_flow_predict", connectionProperties);

		//显示jdbcDF数据内容
		jdbcDF.show();
	}
	def testPgconnet() {
		val conf = new SparkConf().setAppName("Word2Vec example").setMaster("local[2]")
		val sc = new SparkContext(conf)
		val sqlContext = new SQLContext(sc)
		val schema = StructType(List(
			StructField("ruleid", StringType, nullable = false),
			StructField("rule_time", LongType, nullable = true),
			StructField("match_time", LongType, nullable = true)
		))

		val rdd = sc.parallelize(Seq(
			Row("fds23", 1516678422.toLong, 1516678422.toLong),
			Row("3213fds23", 1516678422.toLong, 1516678422.toLong),
			Row("f32144444ds23", 1516678422.toLong, 1516678422.toLong)
		))

		val df = sqlContext.createDataFrame(rdd, schema)
		df.foreachPartition(rdd => {
			val conn = PgSqlUtil.getConn()
			rdd.foreach(row => {
				val ruleid = row.getAs[String]("ruleid")
				val rule_time = new Timestamp(row.getAs[Long]("rule_time") * 1000)
				val match_time = new Timestamp(row.getAs[Long]("match_time") * 1000)
				val prep = conn.prepareStatement(
					"""INSERT INTO public.flowlog_rule(ruleid,rule_time,match_time)  VALUES (?,?, ?); """
				)
				prep.setString(1, ruleid)
				prep.setTimestamp(2, rule_time)
				prep.setTimestamp(3, match_time)
				prep.execute()
			})
			PgSqlUtil.releaseCon(conn)
		})
		PgSqlUtil.destory()
	}
	def deal(patterns: Seq[RegPattern]): Unit = {

		val log = """917879265|!1|!跨站请求伪造事件|!csrf|!/att/vul|!2|!5|!0|!0|!|!|!152.26.242.109|!|!17914|!0|!|!|!172.16.6.212|!|!80|!0|!|!|!|!|!0|!0|!|!/security/WAF|!172.16.42.17|!1513709170000|!1513709170924|!172.16.92.40|!0|!0|!0|!0.0|!4|!|!0|!0|!0|!0|!0.0|!0.0|!|!GET|!|!|!|!|!0|!0|!0|!0|!0|!0|!0|!0|!|!绿盟|!WAF|!跨站请求伪造|!Nsfocus-WAF-1|!|!172.16.6.212/webdav|!waf-g2 waf: tag:waf_log_csrf stat_time:2017-12-20 02:45:25  event_type:csrf  dst_ip:172.16.6.212  dst_port:80  url:172.16.6.212/webdav  src_ip:152.26.242.109  src_port:17914  method:GET  policy_id:37496593  action:Block  count_num:1"""
		val a = run(patterns, log)
		//println(a)
	}
	def run(patterns: Seq[RegPattern], log: String): List[Map[String, Any]] = {

		val pattern = for {
			p <- patterns
			val reg = new Regex(p.pattern)
			if reg.findFirstIn(log).getOrElse(0) != 0
		} yield p
		val (reportIp, orglog) = getReportIpAndOrglog(log)
		val syslog_recordid = UUID.randomUUID().toString()
		if (pattern.length == 0) { //没有匹配到日志，返回syslog
			List(buildSyslog(log, reportIp, syslog_recordid, 0))
		} else { //匹配到日志了,生成genlog，按理所应该只匹配到一条pattern,所以这里是pattern(0)
			List(buildSyslog(log, reportIp, syslog_recordid, 1), buildGenlog(log, reportIp, syslog_recordid, orglog, pattern(0)))
		}
	}
	def getReportIpAndOrglog(log: String): (String, String) = {
		//return (ReportIp,Orglog)
		val s = log.split("""\|\!""")
		val l = s.length
		val reportip = s(29)
		if (s(l - 1).length() < 5) {
			(reportip, s(l - 2))
		} else {
			(reportip, s(l - 1))
		}

	}
	def buildGenlog(log: String, reportIp: String, syslog_recordid: String, orglog: String, regPattern: RegPattern): Map[String, Any] = {
		val reg = new Regex(regPattern.pattern)
		reg.findFirstMatchIn(log) match {
			case Some(s) => {
				var res_map: Map[String, Any] = Map(
					"es_index" -> "genlog",
					"es_type" -> "genlog",
					"reportapp" -> "SYSLOG_LOG",
					"logruleid" -> regPattern.id,
					"reportip" -> reportIp,
					"orglog" -> orglog,
					"orgid" -> syslog_recordid,
					"recordid" -> UUID.randomUUID().toString(),
					"row" -> " ",
					"recordid" -> " ",
					"actionresult" -> " ",
					"firstrecvtime" -> " ",
					"reprotapp" -> " ",
					"reportid" -> " ",
					"sourceip" -> " ",
					"sourceport" -> " ",
					"destport" -> " ",
					"destip" -> " ",
					"eventaction" -> " ",
					"reportnetype" -> " ",
					"eventdefid" -> " ",
					"eventname" -> " ",
					"eventname2" -> " ",
					"eventname3" -> " ",
					"appproto" -> " ",
					"getparameter" -> " ",
					"orglog" -> " ",
					"logruleid" -> " ",
					"eventlevel" -> " ",
					"orgid" -> " ",
					"url" -> " ",
					"getparameter" -> " "
				)
				for (field <- regPattern.fields) {
					val k = field._1
					val v = s.group(Integer.parseInt(field._2))
					if (k == "firstrecvtime" && v.isInstanceOf[String]) { //时间统一成时间撮
						res_map = res_map + (k -> TimeUtil.convert2stamp(v))
					} else if (k == "eventname") { //给eventname2 取值 eventname的 hashcode
						val v_hashcoe = v.hashCode().toString() + "hashcode"
						res_map = res_map + ("eventname" -> v)
						res_map = res_map + ("eventname3" -> v_hashcoe)
					} else {
						res_map = res_map + (field._1 -> s.group(Integer.parseInt(field._2)))
					}

				}

				res_map
			}
			case None => Map()
		}
	}
	def buildSyslog(log: String, reportIp: String, recordid: String, ismatch: Int): Map[String, Any] = {
		val time_stamp = (new Date().getTime + "").toLong
		val syslog_map = Map(
			"es_index" -> "syslog",
			"es_type" -> "syslog",
			"recvtime" -> time_stamp,
			"storagetime" -> time_stamp,
			"reportapp" -> "SYSLOG_LOG",
			"reportip" -> reportIp,
			"row" -> " ",
			"recordid" -> recordid,
			"ismatch" -> ismatch,
			"logcontent" -> log
		)
		syslog_map
	}

}