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
object NetflowProcess {
  def run(log: String): List[Map[String, Any]] = {
    val pattern = new Regex("""send a message:.\[(\[.*\])\]""")
    pattern.findFirstMatchIn(log) match {
      case Some(s) => {
        val json_str = s.group(1)
        val json_obj = parse(json_str)
        implicit val formats = Serialization.formats(ShortTypeHints(List()))
        for {
          JObject(child) <- json_obj

        } yield {
          var map = JObject(child).extract[Map[String, Any]]
          map=mapKey2LowerCase(map)
          map = map + ("recordid" -> " ")
          map = map + ("es_type" -> "netflow")
          map = map + ("es_index" -> "netflow")
          map
        }
      }
      case None=>List()
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