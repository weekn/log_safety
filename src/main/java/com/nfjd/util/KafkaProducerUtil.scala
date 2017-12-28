package com.nfjd.util
import java.util.Properties

import org.apache.kafka.clients.producer.{ KafkaProducer, ProducerRecord }
class KafkaProducerUtil {
  val props = new Properties()
		val TOPIC = "test"
//		props.put("bootstrap.servers", "192.168.181.234:9092")
//		props.put("metadata.broker.list", "192.168.181.234:9092")
		props.put("bootstrap.servers", "172.17.17.21:9092")
		props.put("metadata.broker.list", "172.17.17.21:9092")
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
		
		props.put("group.id", "something")
		val producer = new KafkaProducer[String, String](props)
  def close():Unit={
		producer.close()
  }
  def send(key:String,value:String):Unit={
    val record = new ProducerRecord(TOPIC,key,value)
    producer.send(record)
  }
}