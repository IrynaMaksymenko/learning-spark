package ex5

import org.apache.commons.lang.StringUtils.{isNotEmpty, rightPad}
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

    val collectedData = vacancies
      .na.drop(Seq("address", "name", "salary")) // skip rows with null in any of that columns
      .select("address.city", "name", "salary.from", "salary.to", "salary.currency")
      .na.drop(Seq("city", "currency")) // skip rows with null in any of that columns
      .na.fill(0) // fill null numbers by 0s
      // take only interesting vacations from all data set
      .flatMap(parseInterestingVacations)
      // create map vacancy(city and position) -> (salary, counter)
      .map(row => (Vacancy(row.city, row.position), (row.salary, 1)))
      // sum up salaries by city and position
      .reduceByKey((s1, s2) => (s1._1 + s2._1, s1._2 + s2._2))
      // calculate average dividing sum by counter
      .mapValues { case (sum, count) => (sum / count, count) }
      // return result to driver program
      .collect()

    // print sorted collected data in some kind of table
    println("|______City______|____Position____|_Avg_Salary_USD_|Num_of_vacations|")
    collectedData
      .sortBy(vc => (vc._1.city, vc._1.position))
      .foreach(vc => println("|%s|%s|%s|%s|".format(
        rightPad(vc._1.city, 16, ' '),
        rightPad(vc._1.position, 16, ' '),
        rightPad(vc._2._1.toString, 16, ' '), // avg salary
        rightPad(vc._2._2.toString, 16, ' ')))) // vacations count

  }

  case class Vacancy(city: String, position: String)

  case class DataRow(city: String, position: String, salary: Long)

  def parseInterestingVacations(row: Row): Option[DataRow] = {
    val city: String = row.getString(0)
    val position = parsePosition(row.getString(1))
    val salary = parseSalary(row.getLong(2), row.getLong(3), row.getString(4))
    if (isNotEmpty(city) && !position.equals("other") && salary > 0) Some(DataRow(city, position, salary))
    else None
  }

  def parseSalary(from: Long, to: Long, currency: String): Long = {
    // take average between min and max salary
    // or bigger of two values if one of them is 0
    val salary = Math.max(
      if (from > 0 && to > 0) (from + to) / 2
      else Math.max(from, to),
      0)

    // convert everything to USD
    if ("USD".equals(currency)) salary
    else if ("RUR".equals(currency)) salary / 64
    else 0
  }

  // some positions can have different names but same meaning
  def parsePosition(arg: String): String = {
    val lowerCase = arg.toLowerCase
    if (lowerCase.contains("developer") || lowerCase.contains("программист") || lowerCase.contains("разработчик"))
      "developer"
    else if (lowerCase.contains("designer") || lowerCase.contains("дизайнер"))
      "designer"
    else if (lowerCase.contains("system administrator") || lowerCase.contains("системный администратор"))
      "admin"
    else "other"
  }

}
