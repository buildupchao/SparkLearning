package spark.streaming.streaming2kafka

import java.util

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import scala.util.Random

object MyProducer2 {

  def main(args: Array[String]): Unit = {
    val topic = "logTopic"
    val brokers = "master:9092,slave1:9092"
    val messagePerSec = 10
    val wordsPerMessage = 10

    val props = new util.HashMap[String, Object]()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)
    while (true) {
      (1 to messagePerSec.toInt).foreach({messageNum =>
        val str = (1 to wordsPerMessage.toInt).map(x => Random.nextInt(10).toString).mkString(" ")
        val message = new ProducerRecord[String, String](topic, null, str)
        producer.send(message)
      })
      Thread.sleep(1000)
    }

    producer.close()
  }
}
