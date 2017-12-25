package com.nfjd
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

class SyslogPorcess {

  def run(patterns: Seq[RegPattern], log: String): List[Map[String,Any]] = {
    val pattern = for {
      p <- patterns
      val reg = new Regex(p.pattern)
      if reg.findFirstIn(log).getOrElse(0) != 0
    } yield p
    if (pattern.length == 0) { //没有匹配到日志，返回syslog
      List(buildSyslog(log,0))
    } else { //匹配到日志了,生成genlog，按理所应该只匹配到一条pattern,所以这里是pattern(0)
      List(buildSyslog(log,1),buildGenlog(log,pattern(0)))
    }
  }
  def buildGenlog(log:String,regPattern:RegPattern):Map[String,Any]={
    val reg = new Regex(regPattern.pattern)
      reg.findFirstMatchIn(log) match {
        case Some(s) => {
          var res_map: Map[String, String] = Map("test" -> "test")
          for (field <- regPattern.fields) {
            res_map = res_map + (field._1 -> s.group(Integer.parseInt(field._2)))
          }
          res_map
        }
        case None => Map()
      }
  }
  def buildSyslog(log:String,ismatch:Int): Map[String,Any]= {
    val time_stamp = (new Date().getTime+"").toLong
    val reportip=log.split("""\|\!""")(29)
    val syslog_map=Map(
        "es_type"->"syslog",
        "recvtime"->time_stamp,
        "storagetime"->time_stamp,
        "reportapp"->"SYSLOG_LOG",
        "reportip"->reportip,
        "row"->" ",
        "ismatch"->ismatch,
        "logcontent"->log
    )
    syslog_map
  }

}