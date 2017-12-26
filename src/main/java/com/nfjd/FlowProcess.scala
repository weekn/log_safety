package com.nfjd
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
  def run(log:String):List[Map[String, Any]]={
      val pattern = new Regex("""netlog_http.(\{.*\})""")
      val json_str=pattern.findFirstMatchIn(log).get.group(1)
      implicit val formats = Serialization.formats(ShortTypeHints(List())) 
      var map=parse(json_str).extract[Map[String,Any]]
      map=map+("es_type"->"flow")
      List(map)
  }
}