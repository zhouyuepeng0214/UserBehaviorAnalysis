package com.atguigu.hotitems_analysis

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.io.BufferedSource

object KafkaProducerX {

  def main(args: Array[String]): Unit = {

    writeToKafka("hotitems")

  }

  def writeToKafka(topic : String): Unit = {
    val properties = new Properties()

    properties.setProperty("bootstrap.servers", "hadoop110:9092")
    properties.setProperty("key.serializer",
      "org.apache.kafka.common.serialization.StringSerializer")
    properties.setProperty("value.serializer",
      "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String,String](properties)

    val bufferedSouurce: BufferedSource = io.Source.fromFile("D:\\MyWork\\IdeaProjects\\flink\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")

    for (elem <- bufferedSouurce.getLines()) {
      val record = new ProducerRecord[String,String](topic,elem)
      producer.send(record)
    }

    producer.close
  }

}
