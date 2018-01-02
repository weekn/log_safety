package com.nfjd.etl
import scala.util.matching.Regex
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
object FlowProcess {
  val exclude_uri="""\.css|\.js|\.png|\.jpg|\.bmp""".r
  def run(log:String):List[Map[String, Any]]={
      val pattern = new Regex("""netlog_http.(\{.*\})""")
      val json_str=pattern.findFirstMatchIn(log).get.group(1)
      implicit val formats = Serialization.formats(ShortTypeHints(List())) 
      var map=parse(json_str).extract[Map[String,Any]]
      val uri=map.apply("Uri").asInstanceOf[String]
      if(uri.indexOf(".jsp")<0 &&exclude_uri.findFirstIn(uri).getOrElse(0)==0){
        map=mapKey2LowerCase(map)
        map=map+("es_type"->"flow")
        map=map+("es_index"->"flow")
        List(map)
      }else{
        List()
      }
      
  }
  def mapKey2LowerCase(map:Map[String, Any]):Map[String, Any]={
    var res_map:Map[String, Any]=Map()
    for(k<-map.keySet){
      res_map=res_map+(k.toLowerCase()->map.apply(k))
    }
    res_map
  }
}