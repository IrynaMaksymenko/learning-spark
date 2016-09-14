package ex7

import net.sf.uadetector.service.UADetectorServiceFactory
import net.sf.uadetector.{ReadableUserAgent, UserAgentStringParser, UserAgentType}
import org.joda.time.DateTime
import org.squeryl.PrimitiveTypeMode._
import utils.{S3Utils, SparkUtils}

object BrowserUsageStatistics {

  // S3 configuration
  val amplabS3Bucket = "big-data-benchmark"
  val dataSetName = "uservisits"
  val dataSetSize = "tiny"
  val amplabS3Path = s"pavlo/text/$dataSetSize/$dataSetName"
  val filesToLoad = 10

  val parser: UserAgentStringParser = UADetectorServiceFactory.getResourceModuleParser

  def main(args: Array[String]) {

    // PREPARATION
    val sc = SparkUtils.createSparkContext("ex7")
    val s3Credentials = S3Utils.getS3Credentials
    // List S3 files to load
    val s3Files = S3Utils.s3ListChildren(amplabS3Bucket, amplabS3Path, s3Credentials)
      .take(filesToLoad)
      .map(key => s"s3n://$amplabS3Bucket/$key")
    S3Utils.configureHadoopForS3(sc, s3Credentials)

    // ACTUAL WORK

    // Load files from S3
    val logRecords = sc.union(s3Files.map(sc.textFile(_)))

    val statistics = logRecords
      // split line into words
      .map((row) => row.split(","))
      // parse browser and month values
      .flatMap {
      case Array(sourceIP, destURL, visitDate, adRevenue, userAgent, countryCode, languageCode, searchWord, duration)
      => parseStatistic(visitDate, userAgent)
      case unknown
      => None
    }
      // add counter
      .map(browserVisit => browserVisit -> 1)
      // sum up by key
      .reduceByKey(_ + _)
      // save counted into usage field
      .map(pair => {
      pair._1.usage = pair._2
      pair._1
    })
      .collect()

    // OPERATIONS WITH DATABASE

    // connect to database
    DatabaseUtils.openSession()
    DatabaseUtils.createSchemaIfNotExists()

    // replace calculated statistics
    transaction {
      DatabaseSchema.STATISTICS.deleteWhere(s => s.month gte 0)
      DatabaseSchema.STATISTICS.insert(statistics)
    }

    // select statistics for January (as an example)
    transaction {
      println("Statistic for January:")
      from(DatabaseSchema.STATISTICS)(s => where(s.month === 1) select s orderBy (s.usage desc))
        .foreach(s => printStatistic(s))
    }
  }

  // ----------------  safe parsing  -------------------------------------

  def parseStatistic(visitDate: String, userAgent: String): Option[Statistic] = {
    try {
      val readableUserAgent: ReadableUserAgent = parser.parse(userAgent)
      parseBrowser(readableUserAgent) match {
        case Some(browser) =>
          parseMonth(visitDate) match {
            case Some(monthIndex) =>
              parseOS(readableUserAgent) match {
                case Some(os) => Some(Statistic(monthIndex, browser, os, 0))
                case None => None
              }
            case None => None
          }
        case None => None
      }
    } catch {
      case e: Exception => None
    }
  }

  def parseBrowser(userAgent: ReadableUserAgent): Option[String] = {
    if (userAgent.getType == UserAgentType.BROWSER) Some(userAgent.getName)
    else None
  }

  def parseOS(userAgent: ReadableUserAgent): Option[String] = {
    if (userAgent.getType == UserAgentType.BROWSER) Some(userAgent.getOperatingSystem.getName)
    else None
  }

  // convert month into index
  def parseMonth(visitDate: String): Option[Int] = {
    try {
      Some(DateTime.parse(visitDate).getMonthOfYear)
    } catch {
      case e: Exception => None
    }
  }

  // --------------  printing  -----------------------------------------------

  def printStatistic(statistic: Statistic) = {
    println(s"${toMonthName(statistic.month)}, ${statistic.browser}, ${statistic.os}, ${statistic.usage}")
  }

  def toMonthName(monthIndex: Int): String = {
    monthIndex match {
      case 1 => "January"
      case 2 => "February"
      case 3 => "March"
      case 4 => "April"
      case 5 => "May"
      case 6 => "June"
      case 7 => "July"
      case 8 => "August"
      case 9 => "September"
      case 10 => "October"
      case 11 => "November"
      case 12 => "December"
    }
  }

}
