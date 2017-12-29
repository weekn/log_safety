package com.nfjd.util
import java.util.Locale
import java.text.SimpleDateFormat
import java.util.Date
object TimeUtil {
  val loc = new Locale("en")
  def getTime():String={
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss") 
    dateFormat.format(new Date())
  }
  
  def convert2stamp(time:String):Long={
		val fms=List(new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss",loc),new SimpleDateFormat("yyyy-MM-dd HH:mm:ss",loc))
		var res:Long= 0
		for(fm<-fms if res==0){
			try{
				res=fm.parse(time).getTime
			}catch{
				case e: Exception => {
					
				}
			}
		}
		res
	}
}