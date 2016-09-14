package exKafka

import java.lang.Double

import kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka._
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import utils.SparkUtils


object KafkaIntegration {

  def main(args: Array[String]) {
    // initialization
    val sc = SparkUtils.createSparkContext("KafkaIntegration")
    val ssc = new StreamingContext(sc, Seconds(3))
    ssc.checkpoint("checkpoint")

    // create stream
    val dStream = KafkaUtils.createDirectStream[String, DeviceStatus, StringDecoder, DeviceStatusSerializer](
      ssc, Map[String, String]("bootstrap.servers" -> "localhost:9092"), Set("device-status"))

    dStream
      .map(_._2.getValue) // map to message and take only 'value' from it
      .map(value => Aggregated(value, value, value, 1))
      // aggregate in windows of 1 minute length
      // sliding is also 1 minute, so that data for computations do not overlap
      .reduceByWindow((a, b) =>
        Aggregated(math.min(a.min, b.min), math.max(a.max, b.max), a.sum + b.sum, a.count + b.count),
        Minutes(1), Minutes(1))
      // send result of each window to new kafka topic
      .foreachRDD(rdd => rdd.foreach {
            // this will be executed on worker
            // there will be one instance of KafkaProducer created on each worker
            // that is why StatisticsProducer should be static
        record => StatisticsProducer.send(new Statistics(record.min, record.max, record.sum / record.count))
      })

    // start the computation
    ssc.start()
    ssc.awaitTermination()

  }

  case class Aggregated(min: Double, max: Double, sum: Double, count: Int)

}
