package exKafka

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

object StatisticsProducer {

  val kafkaProducer = new KafkaProducer[String, Statistics](
    KafkaIntegrationUtils.getProducerConfig, new StringSerializer(), new StatisticsSerializer())

  def send(statistics: Statistics) = {
    kafkaProducer.send(new ProducerRecord[String, Statistics]("statistics", statistics))
  }

}
