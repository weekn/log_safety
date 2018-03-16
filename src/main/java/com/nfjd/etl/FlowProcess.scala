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
import java.util.UUID
import org.apache.spark.broadcast.Broadcast
import scala.collection.mutable.ArrayBuffer
import java.net.URLDecoder
import scala.collection.mutable.{ Map => MutableMap }
object FlowProcess {
	val exclude_uri = """\.css|\.js|\.png|\.jpg|\.bmp|\.ico""".r
	//val exclude_uri = """\.css""".r

	def run(log: String, flowlogRule: Broadcast[ArrayBuffer[Map[String, String]]]): List[MutableMap[String, Any]] = {
		val pattern = new Regex("""(\{.*\})""")
		pattern.findFirstIn(log) match {
			case Some(s) =>successMatch(s,flowlogRule)
			case None=>List()
		}

	}
	def successMatch(s: String,flowlogRule: Broadcast[ArrayBuffer[Map[String, String]]]): List[MutableMap[String, Any]] = {
		implicit val formats = Serialization.formats(ShortTypeHints(List()))
		val map = parse(s).extract[Map[String, Any]]
		val uri=map.getOrElse("Uri", " ").toString()
		if (uri.indexOf(".jsp") < 0 && exclude_uri.findFirstIn(uri).getOrElse(0) == 0) {
			val res_map = completeMap(map)
			try {
				val matched_ruleid = matchFlowlogRule(res_map, uri, flowlogRule)
				if (matched_ruleid.length() > 0) {
					List(res_map, res_map + ("ruleid" -> matched_ruleid, "es_type" -> "flowlogrule", "es_index" -> "flowlogrule"))
				} else {
					List(res_map)
				}
			} catch {
				case ex: Exception =>
					{
						println(ex)
					}
					List(res_map)

			}

		} else {
			List()
		}
	}
	def matchFlowlogRule(
			map: MutableMap[String, Any],
			uri: String,
			flowlogRule: Broadcast[ArrayBuffer[Map[String, String]]]
	): String = {
		val rules = flowlogRule.value
		val uir_decoded = URLDecoder.decode(uri.replaceAll("%(?![0-9a-fA-F]{2})", "%25")).toLowerCase()
		val log_srcip = map.apply("srcip")
		val log_dstip = map.apply("dstip")
		var ifgo = true
		var matched_ruleid = ""
		for (rule <- rules if ifgo) {
			val pattern = rule.apply("rule_regular_expression").r
			val destip = rule.apply("destip")
			val sourceip = rule.apply("sourceip")
			if (log_srcip == sourceip || sourceip.indexOf("""\.""") < 0) {
				if (log_dstip == destip || destip.indexOf("""\.""") < 0) {
					if (pattern.findFirstIn(uir_decoded).getOrElse(0) != 0) {
						ifgo = false
						matched_ruleid = rule.apply("ruleid")
					}
				}
			}
		}
		matched_ruleid
	}
	def completeMap(map: Map[String, Any]): MutableMap[String, Any] = {
		val res_map: MutableMap[String, Any] = getAllKeyMap()
		res_map += ("flowcontent" -> map)

		for (k <- map.keySet) {
			val key = k.toLowerCase()
			if (key == "date") {
				res_map += (key -> TimeUtil.convert2stamp(map.apply(k).asInstanceOf[String]))
			} else {
				res_map += (key -> map.apply(k))
			}

		}

		res_map
	}
	def getAllKeyMap(): MutableMap[String, Any] = {
		val all_key_map: MutableMap[String, Any] = MutableMap(
			"id" -> UUID.randomUUID().toString(),
			"es_type" -> "flow",
			"es_index" -> "flow",
			"host" -> " ",
			"mailattach" -> " ",
			"mailattachpath" -> " ",
			"mailbcc" -> " ",
			"mailcc" -> " ",
			"mailcontent" -> " ",
			"mailcontentpath" -> " ",
			"mailfrom" -> " ",
			"mailsubject" -> " ",
			"mailto" -> " ",
			"method" -> " ",
			"para" -> " ",
			"password" -> " ",
			"recvid" -> " ",
			"row" -> " ",
			"sendid" -> " ",
			"sql" -> " ",
			"srcip" -> " ",
			"srcmac" -> " ",
			"srcport" -> " ",
			"title" -> " ",
			"type" -> " ",
			"uri" -> " ",
			"username" -> " ",
			"action" -> " ",
			"appproto" -> " ",
			"database" -> " ",
			"date" -> " ",
			"dbtype" -> " ",
			"descr" -> " ",
			"display" -> " ",
			"dstip" -> " ",
			"dstmac" -> " ",
			"dstport" -> " ",
			"filename" -> " "
		)
		all_key_map
	}
}