package com.nfjd.etl
import org.json4s.Extraction
import org.json4s.JObject
import org.json4s.JsonDSL.list2jvalue
import org.json4s.ShortTypeHints
import org.json4s.jackson.JsonMethods.compact
import org.json4s.jackson.JsonMethods.parse
import org.json4s.jackson.JsonMethods.render
import org.json4s.jackson.Serialization
import org.json4s.jvalue2monadic
import org.json4s.string2JsonInput
import scala.util.matching.Regex
import com.nfjd.model.RegPattern
import java.text.SimpleDateFormat
import java.util.Date
import com.nfjd.util.TimeUtil

object SyslogProcess {
  val all_key = List("row", "firstrecvtime", "reprotapp", "reportid", "sourceip", "sourceport",
    "destport", "destip", "eventation", "reportnetype", "eventdefid", "rventname", "eventname2",
    "eventname2", "approto", "getparameter", "orglog", "logruleid")
  def run(patterns: Seq[RegPattern], log: String): List[Map[String, Any]] = {
   
    val pattern = for {
      p <- patterns
      val reg = new Regex(p.pattern)
      if reg.findFirstIn(log).getOrElse(0) != 0
    } yield p
    val (reportIp, orglog) = getReportIpAndOrglog(log)
    if (pattern.length == 0) { //没有匹配到日志，返回syslog
      List(buildSyslog(log, reportIp, 0))
    } else { //匹配到日志了,生成genlog，按理所应该只匹配到一条pattern,所以这里是pattern(0)
      List(buildSyslog(log, reportIp, 1), buildGenlog(log, reportIp, orglog, pattern(0)))
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
  def buildGenlog(log: String, reportIp: String, orglog: String, regPattern: RegPattern): Map[String, Any] = {
    val reg = new Regex(regPattern.pattern)
    reg.findFirstMatchIn(log) match {
      case Some(s) => {
        var res_map: Map[String, Any] = Map(
          "es_index" -> "syslog",
          "es_type" -> "genlog",
          "reportapp" -> "SYSLOG_LOG",
          "logruleid" -> regPattern.id,
          "reportip" -> reportIp,
          "orglog" -> orglog)
        for (field <- regPattern.fields) {
          val k=field._1
          val v=s.group(Integer.parseInt(field._2))
          if(k=="firstrecvtime" && v.isInstanceOf[String]){//时间统一成时间撮
            res_map = res_map + (k -> TimeUtil.convert2stamp(v))
          }else{
            res_map = res_map + (field._1 -> s.group(Integer.parseInt(field._2)))
          }
          
        }
        //补充废弃，或缺失的字段
        for (key <- all_key) {
          if (!res_map.contains(key)) {
            if(key=="firstrecvtime"){
              res_map = res_map + (key -> (new Date().getTime + "").toLong)
            }else{
               res_map = res_map + (key -> " ")
            }
           
          }
        }
        res_map
      }
      case None => Map()
    }
  }
  def buildSyslog(log: String, reportIp: String, ismatch: Int): Map[String, Any] = {
    val time_stamp = (new Date().getTime + "").toLong
    val syslog_map = Map(
      "es_index" -> "syslog",
      "es_type" -> "syslog",
      "recvtime" -> time_stamp,
      "storagetime" -> time_stamp,
      "reportapp" -> "SYSLOG_LOG",
      "reportip" -> reportIp,
      "row" -> " ",
      "ismatch" -> ismatch,
      "logcontent" -> log)
    syslog_map
  }

}