package nfjd.sparkstm
import scala.util.matching.Regex
import scala.xml.XML

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.elasticsearch.spark.streaming.sparkDStreamFunctions
import org.elasticsearch.spark.streaming.sparkStringJsonDStreamFunctions
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

import com.nfjd.SyslogPorcess
import com.nfjd.model.RegPattern
//com.nfjd.LogEtlStm
object LogEtlStm {
  case class Syslog(
    firstrecvtime: String,
    reportapp: String,
    reportip: String,
    sourceip: String,
    sourceport: String,
    destip: String,
    destport: String,
    eventname: String,
    eventlevel: String,
    orgid: String)
 // case class Pattern(pattern: String, fields: Seq[(String, String)])
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("nfjd-log-etl").set("es.nodes", "172.17.17.30").set("es.port", "9200").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(1))

    val kafkaParams = Map[String, Object](
      // "bootstrap.servers" -> "192.168.181.234:9092", //"172.17.17.21:9092,172.17.17.22:9092",
      "bootstrap.servers" -> "172.17.17.21:9092,172.17.17.22:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "1",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean))
    //val topics = Array("flow_demo", "netflow_demo","syslog_safety_demo","zwj_test")
    val (topics, patterns) = readConfig()
    val broadcastVar = ssc.sparkContext.broadcast(patterns)

    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams))
    val data = stream.map(record => record.value)

    dealFlow(data)
    dealNetFlow(data)
    dealSysLog(broadcastVar, data)
    ssc.start()
    ssc.awaitTermination()
  }
  def readConfig(): (Array[String], Seq[RegPattern]) = {
    val xml = XML.loadFile("config.xml")
    val topics = (xml \ "kafka_topics" \ "topic").map(x => x.text).toArray

    val patterns = (xml \ "patterns" \ "pattern").map(x => {

      RegPattern(pattern = (x \ "content").text, fields = (x \ "field").map(s => {
        ((s \ "@id").text, (s \ "@group").text)
      }))
    })
    (topics, patterns)
  }

  def dealSysLog(patterns_bro: Broadcast[Seq[RegPattern]], data: DStream[String]): Unit = {
   
    val rr = data.flatMap(log => new SyslogPorcess().run(patterns_bro.value,log)).filter(x => x != "none")
    rr.print()
    rr.saveToEs("spark_test/syslog")
  }
  def dealFlow(data: DStream[String]): Unit = {

    data.filter(line => {
      val pattern = """netlog_http.\{.*\}""".r
      pattern.findFirstIn(line) match {
        case Some(s) => true
        case None => false
      }
    }).map(line => {
      val pattern = new Regex("""netlog_http.(\{.*\})""")
      pattern.findFirstMatchIn(line).get.group(1)
    }).saveJsonToEs("spark_test/flow")
  }

  def dealNetFlow(lines: DStream[String]): Unit = {
    lines.filter(f => {
      val pattern = """send a message:.\[(\[.*\])\]""".r
      pattern.findFirstIn(f) match {
        case Some(s) => true
        case None => false
      }
    }).map(line => {
      val pattern = new Regex("""send a message:.\[(\[.*\])\]""")
      val res = pattern.findFirstMatchIn(line)
      res.get.group(1)
    }).flatMap(x => {
      val json_obj = parse(x, useBigDecimalForDouble = true)
      for {
        JObject(child) <- json_obj

      } yield compact(render(child))

    }).saveJsonToEs("spark_test/netflow")
  }

  def dealSyslog_invadeEvent(line: DStream[String]): Unit = {
    val etl_res1 = line.filter(f => {
      val pattern = """.*?\|\!((?:\d{1,3}\.){3}\d{1,3})\|.*\{.*"time":(.*),"source":\{"\w+":"(.*)","port":(\d+),.*,"destination":\{"\w+":"(.*)","port":(\d+),.*\},.*"protocol":"(.*)","subject":"(.*)","message":.*""".r
      val res = pattern findFirstIn f
      res match {
        case Some(s) => true
        case None => false
      }
    })
    val etl_res = etl_res1.map(line => {
      val pattern = new Regex(""".*?\|\!((?:\d{1,3}\.){3}\d{1,3})\|.*\{.*"time":(.*),"source":\{"\w+":"(.*)","port":(\d+),.*,"destination":\{"\w+":"(.*)","port":(\d+),.*\},.*"protocol":"(.*)","subject":"(.*)","message":.*""")
      val res = pattern findFirstMatchIn line
      res match {
        case Some(s) => {

          val sysLog = Syslog(
            firstrecvtime = s.group(1),
            reportapp = s.group(1),
            reportip = s.group(1),
            sourceip = s.group(3),
            sourceport = s.group(4),
            destip = s.group(5),
            destport = s.group(6),
            eventname = s.group(7),
            eventlevel = s.group(8),
            orgid = s.group(1))
          implicit val formats = Serialization.formats(ShortTypeHints(List()))
          compact(Extraction.decompose(sysLog))
        }
        case None => "none"
      }

    })
    etl_res.saveJsonToEs("spark_test/syslog")
    etl_res1.print()
    etl_res.print()
  }
}