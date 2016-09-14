package utils

import java.util.Properties

import com.google.common.base.Strings
import org.apache.spark.{SparkConf, SparkContext}

object SparkUtils {

  def createSparkContext(appName: String): SparkContext = {
    // load configuration properties
    val configuration: Properties = new Properties()
    configuration.load(getClass.getClassLoader.getResourceAsStream("conf.properties"))

    val masterUrl: String = configuration.getProperty("spark.master.url")
    val pathToJar: String = configuration.getProperty("path.to.jar")

    // set up spark context
    val conf = new SparkConf()
      .setAppName(appName)
      .set("spark.executor.memory", "4g")
      //.set("spark.driver.maxResultSize", "2g")
      //.set("spark.default.parallelism", "1000")
      //.set("spark.rpc.message.maxSize", "2560")
      //.set("spark.broadcast.blockSize", "50m")
      //.set("spark.task.cpus", "8")
    if (!Strings.isNullOrEmpty(masterUrl))
      conf.setMaster(masterUrl)
    if (!Strings.isNullOrEmpty(pathToJar))
      conf.setJars(Seq(pathToJar))

    new SparkContext(conf)
  }

  def getConfigurationProperty(alias: String) = {
    val configuration: Properties = new Properties()
    configuration.load(getClass.getClassLoader.getResourceAsStream("conf.properties"))
    configuration.getProperty(alias)
  }

}
