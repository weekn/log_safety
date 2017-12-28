package com.nfjd.util
import java.text.SimpleDateFormat  
import java.util.Date 
object TimeUtil {
  def getTime():String={
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss") 
    dateFormat.format(new Date())
  }
}