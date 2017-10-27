package spark.streaming.streaming2kafka

import java.util.Properties

import kafka.javaapi.producer.Producer
import kafka.producer.{KeyedMessage, ProducerConfig}

object MyProducer {
  def main(args: Array[String]): Unit = {
    val props = new Properties()
    props.setProperty("metadata.broker.list", "localhost:9092")
    props.setProperty("serializer.class", "kafka.serializer.StringEncoder")
    props.put("request.required.acks", "1")
    val config = new ProducerConfig(props)

    // Create a producer object
    val producer = new Producer[String, String](config)
    // produce message
    val data1 = new KeyedMessage[String, String]("top1", "test kafka")
    val data2 = new KeyedMessage[String, String]("top2", "hello world")

    var i = 1
    while (i < 100) {
      producer.send(data1)
      producer.send(data2)
      i += 1
      Thread.sleep(1000)
    }

    producer.close
  }
}
