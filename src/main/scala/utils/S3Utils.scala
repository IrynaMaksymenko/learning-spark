package utils

import java.util.Properties

import com.amazonaws.auth.{AWSCredentials, DefaultAWSCredentialsProviderChain}
import com.amazonaws.services.s3.AmazonS3Client
import com.google.common.base.Strings
import org.apache.spark.SparkContext

object S3Utils {

  def getS3Credentials: AWSCredentials = {
    // load configuration properties
    val configuration: Properties = new Properties()
    configuration.load(getClass.getClassLoader.getResourceAsStream("conf.properties"))

    // load aws credentials from properties file if they are defined there
    val accessKeyId: String = configuration.getProperty("aws.accessKeyId")
    val secretKey: String = configuration.getProperty("aws.secretKey")

    if (!Strings.isNullOrEmpty(accessKeyId) && !Strings.isNullOrEmpty(secretKey)) {
      val props = new Properties(System.getProperties)
      props.setProperty("aws.accessKeyId", accessKeyId)
      props.setProperty("aws.secretKey", secretKey)
      System.setProperties(props)
    }

    new DefaultAWSCredentialsProviderChain().getCredentials
  }

  def s3ListChildren(bucket: String, path: String, s3Credentials: AWSCredentials): List[String] = {
    import scala.collection.JavaConversions._

    val client = new AmazonS3Client(s3Credentials)
    client.setEndpoint("s3.amazonaws.com")

    client
      .listObjects(bucket, path)
      .getObjectSummaries.toList
      .map(s => s.getKey)
      .filter(!_.contains("$folder$"))
  }

  def configureHadoopForS3(sc: SparkContext, s3Credentials: AWSCredentials) = {
    val hadoopCfg = sc.hadoopConfiguration
    hadoopCfg.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    hadoopCfg.set("fs.s3n.awsAccessKeyId", s3Credentials.getAWSAccessKeyId)
    hadoopCfg.set("fs.s3n.awsSecretAccessKey", s3Credentials.getAWSSecretKey)
    hadoopCfg
  }

}
