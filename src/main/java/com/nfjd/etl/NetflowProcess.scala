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
import java.util.UUID

object NetflowProcess {
	def run(log: String): List[Map[String, Any]] = {
		val pattern = new Regex("""send a message:.\[(\[.*\])\]""")
		pattern.findFirstMatchIn(log) match {
			case Some(s) => {
				val json_str = s.group(1)
				val json_obj = parse(json_str)
				implicit val formats = Serialization.formats(ShortTypeHints(List()))
				val res = parse(json_str).extract[List[Map[String, Any]]]
				for {
					m <- res
				} yield {
					var map = m
					map = mapKey2LowerCase(map)

					map = map + ("recordid" -> UUID.randomUUID().toString())
					map = map + ("es_type" -> "netflow")
					map = map + ("es_index" -> "netflow")
					try{
						val packernum=map.getOrElse("downpkts", 0).asInstanceOf[BigInt]+map.getOrElse("uppkts", 0).asInstanceOf[BigInt]
						map=map+("packernum" ->packernum)
					}catch{
						case e:Exception=>{
							println("???只是加个字段而已，就是加个字段而已，为了之后的分析用而已")
							println(e.getMessage)
							println(e.getStackTrace)
							//e.printStackTrace()
						}
					}
					
					map
				}
			}
			case None => List()
		}

	}
	def mapKey2LowerCase(map: Map[String, Any]): Map[String, Any] = {
		
		var res_map: Map[String, Any] = Map()
		for (k <- map.keySet) {
			res_map = res_map + (k.toLowerCase() -> map.apply(k))
		}
		res_map
	}
}