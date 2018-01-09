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
import com.nfjd.util.TimeUtil

object FlowProcess {
  val exclude_uri="""\.css|\.js|\.png|\.jpg|\.bmp|\.ico""".r
  val all_keys:List[String]=List("host" ,"id" ,"mailattach","mailattachpath","mailbcc" ,"mailcc" ,"mailcontent" ,"mailcontentpath" ,"mailfrom",
        "mailsubject" ,"mailto" ,"method","para","password","recvid" ,"row" ,"sendid" ,"sql" ,"srcip","srcmac" ,
        "srcport" ,"title" , "type" , "uri" ,"username","action","appproto" ,"database","date" ,"dbtype","descr" ,
        "display","dstip","dstmac" ,"dstport","filename")
  def run(log:String):List[Map[String, Any]]={
      val pattern = new Regex("""netlog_http.(\{.*\})""")
      val json_str=pattern.findFirstMatchIn(log).get.group(1)
      implicit val formats = Serialization.formats(ShortTypeHints(List())) 
      var map=parse(json_str).extract[Map[String,Any]]
      val uri=map.apply("Uri").asInstanceOf[String]
      if(uri.indexOf(".jsp")<0 &&exclude_uri.findFirstIn(uri).getOrElse(0)==0){

        List(completeMap(map))
      }else{
        List()
      }
      
  }
  def completeMap(map:Map[String, Any]):Map[String, Any]={
    var res_map:Map[String, Any]=Map()
    
    for(k<-map.keySet){
      val key=k.toLowerCase()
      if(key=="date"){
        res_map=res_map+(key->TimeUtil.convert2stamp(map.apply(k).asInstanceOf[String]))
      }else{
        res_map=res_map+(key->map.apply(k))
      }
      
    }
    for(k<-all_keys){
      if(!res_map.contains(k)){
        res_map=res_map+(k->" ")
      }
      
    }
    res_map=res_map+("flowcontent"->map)
    res_map=res_map+("es_type"->"flow")
    res_map=res_map+("es_index"->"flow")
    res_map
  }
}