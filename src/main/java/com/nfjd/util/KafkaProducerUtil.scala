package com.nfjd.util
import java.util.Properties

import org.apache.kafka.clients.producer.{ KafkaProducer, ProducerRecord }
import java.util.ArrayList

class KafkaProducerUtil(topic:String) {
    val props = new Properties()
		
//		props.put("bootstrap.servers", "192.168.181.234:9092")
//		props.put("metadata.broker.list", "192.168.181.234:9092")
		val sever:ArrayList[String]=new ArrayList()
	
		sever.add("172.17.17.22:9092")
		sever.add("172.17.17.23:9092")
		sever.add("172.17.17.21:9092")
		props.put("bootstrap.servers", sever)
		props.put("metadata.broker.list", sever)
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

		props.put("group.id", "something")
		
		val producer = new KafkaProducer[String, String](props)
  def close():Unit={
		producer.close()
  }
  def send(key:String,value:String):Unit={
    val record = new ProducerRecord(topic,key,value)
    producer.send(record)
  }
}