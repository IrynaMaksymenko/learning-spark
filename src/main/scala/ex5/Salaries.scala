package ex5

import org.apache.spark.sql.Row
import utils.SparkUtils

object Salaries {

  def main(args: Array[String]) {

    // PREPARATION

    val sc = SparkUtils.createSparkContext("ex5")
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    // ACTUAL WORK

    // read
    // file is packaged into jar so it must be on classpath
    val vacancies = sqlContext.read.json(getClass.getClassLoader.getResource("hh-vacs").getFile)

    // we are interested only in next 3 positions
    val desiredPositions: List[String] = List("developer", "designer", "admin")

    // define parsing functions
    val getPosition: ((Row, Int) => String) = parsePosition
    val getCity: ((Row, Int) => String) = parseCity
    val getSalary: ((Row, Int) => Long) = parseSalary

    val collectedData = vacancies
      .select("address.city", "name", "salary")
      // parse data and create map vacancy -> salary
      .map(row => (new Vacancy(getCity(row, 0), getPosition(row, 1)), getSalary(row, 2)))
      // filter invalid data
      .filter(vacancyAndSalary =>
        desiredPositions.contains(vacancyAndSalary._1.position)
          && vacancyAndSalary._1.city
          != "" && vacancyAndSalary._2 > 0)
      // add counter
      .mapValues((_, 1))
      // sum up salaries by city and position
      .reduceByKey((s1, s2) => (s1._1 + s2._1, s1._2 + s2._2))
      // calculate average dividing sum by counter
      .mapValues { case (sum, count) => sum / count }
      // return result to driver program
      .collect()

    // print sorted collected data in some kind of table
    println("|______City______|____Position____|_Avg_Salary_USD_|")
    collectedData
      .sortBy(vc => (vc._1.city, vc._1.position))
      .foreach(vc => println("|%s|%s|%s|".format(
        getStringOfLength16(vc._1.city),
        getStringOfLength16(vc._1.position),
        getStringOfLength16(vc._2.toString))))

  }

  case class Vacancy(city: String, position: String)

  def parseSalary(row: Row, index: Int): Long = {
    if (row.isNullAt(index)) return 0l

    val salary: Row = row.getStruct(index)

    val currency = if (salary.isNullAt(0)) "" else salary.getString(0)
    val min = if (salary.isNullAt(1)) 0 else salary.getLong(1)
    val max = if (salary.isNullAt(2)) 0 else salary.getLong(2)

    var avg: Long = 0
    if (min > 0 && max > 0) {
      avg = (min + max) / 2
    } else {
      if (min > 0) {
        avg = min
      } else if (max > 0) {
        avg = max
      }
    }

    if (currency == "USD") avg
    else if (currency == "RUR") avg / 64l
    else 0l
  }

  def parseCity(row: Row, index: Int): String = {
    if (row.isNullAt(index)) "" else row.getString(index)
  }

  // some positions can have different names but same meaning
  def parsePosition(row: Row, index: Int): String = {
    if (row.isNullAt(index)) return ""

    val arg: String = row.getString(index)
    val lowerCase = arg.toLowerCase
    if (lowerCase.contains("developer") || lowerCase.contains("программист") || lowerCase.contains("разработчик"))
      "developer"
    else if (lowerCase.contains("designer") || lowerCase.contains("дизайнер"))
      "designer"
    else if (lowerCase.contains("system administrator") || lowerCase.contains("системный администратор"))
      "admin"
    else "other"
  }

  // converts string to string with predefined length (of 16 symbols)
  def getStringOfLength16(str: String): String = {
    if (str.length() > 16) return str.substring(0, 16)
    var result = str
    val rest = 16-str.length()
    for (i <- 1 to rest) {
      result = result.concat(" ")
    }
    result
  }

}
