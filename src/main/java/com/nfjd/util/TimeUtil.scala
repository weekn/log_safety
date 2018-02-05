package com.nfjd.util
import java.util.Locale
import java.text.SimpleDateFormat
import java.util.Date
object TimeUtil {
  val loc = new Locale("en")
  def getTime(): String = {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    dateFormat.format(new Date())
  }
  def getCurrentTimeStamp(): Long = {
		val now = new Date()
		val a = now.getTime.toString().substring(0,10).toLong
		a
	}
  def convert2stamp(time: String): Long = {
    val if_num_pattern = """^(\d+)$""".r
    if (if_num_pattern.findFirstIn(time).getOrElse(0) != 0) { //不等于0说明是纯数字
      time.toLong
    } else {
      val fms = List(new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", loc), new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", loc))
      var res: Long = 0
      for (fm <- fms if res == 0) {
        try {
          res = fm.parse(time).getTime.toString().substring(0,10).toLong
        } catch {
          case e: Exception => {

          }
        }
      }
      res
    }

  }
}