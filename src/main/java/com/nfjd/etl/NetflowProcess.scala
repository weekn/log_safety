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
          map = map + ("es_type" -> "netflow")
          map
        }
      }
      case None=>List()
    }

  }
}