package ex3

import utils.SparkUtils

object Countries {

  def main(args: Array[String]) {

    // PREPARATION
    val sc = SparkUtils.createSparkContext("ex3")

    // ACTUAL WORK

    // file is packaged into jar so it must be on classpath
    val distFile = sc.textFile(getClass.getClassLoader.getResource("uservisits").getFile)

    distFile
      .map((row) => row.split(","))
      // create map countryCode -> count
      .flatMap {
        case Array(sourceIP, destURL, visitDate, adRevenue, userAgent, countryCode, languageCode, searchWord, duration)
        => Some((countryCode, 1))
        case unknown
        => None
      }
      // sum up count
      .reduceByKey(_ + _)
      // sort descending by count
      .sortBy(_._2, ascending = false)
      .take(10)
      .foreach(println)

  }

}
