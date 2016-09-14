package exKafka

import java.lang

// need to use java.lang.Double for correct serialization using JacksonEncoder
// this is needed only because there is mixture of java and scala
// and I want to reuse custom Double serializer configured in ObjectMapper
class Statistics(min: lang.Double, max: lang.Double, avg: lang.Double) extends Serializable {

  def toSimpleString: String = s"Statistic for a period: min = $min, max = $max, avg = $avg)"

  // need getters for Jackson
  def getMin: lang.Double = min
  def getMax: lang.Double = max
  def getAvg: lang.Double = avg

}
