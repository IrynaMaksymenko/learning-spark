package exKafka

import java.util

import org.apache.kafka.common.serialization.Serializer

class StatisticsSerializer extends JacksonEncoder[Statistics] with Serializer[Statistics] {

  override protected def getSerializedType: Class[Statistics] = classOf[Statistics]

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = ()

  override def serialize(topic: String, data: Statistics): Array[Byte] = toBytes(data)

  override def close(): Unit = ()
}
