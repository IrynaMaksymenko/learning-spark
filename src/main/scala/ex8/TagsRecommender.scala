package ex8

import java.util.concurrent.atomic.AtomicInteger

import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.{FileInputFormat, JobConf}
import org.apache.hadoop.streaming.StreamInputFormat
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.storage.StorageLevel
import utils.SparkUtils

import scala.collection.mutable
import scala.xml.{Elem, XML}

object TagsRecommender {

  def main(args: Array[String]) {
    // PREPARATION
    val sc = SparkUtils.createSparkContext("ex8")

    // file must be located on worker
    val pathToFile = SparkUtils.getConfigurationProperty("path.to.posts.xml")
    val inputFile = s"file://$pathToFile"

    // configure hadoop job for parsing xml
    val jobConf = new JobConf()
    jobConf.set("stream.recordreader.class", "org.apache.hadoop.streaming.StreamXmlRecordReader")
    jobConf.set("stream.recordreader.begin", "<row")
    jobConf.set("stream.recordreader.end", "/>")

    FileInputFormat.addInputPaths(jobConf, inputFile)

    def parseRow(row: String) = {
      val rowElement: Elem = XML.loadString(row)
      val id: Int = (rowElement \ "@Id").text.toInt
      val tags = (rowElement \ "@Tags").text.split(">").transform(s => s.stripPrefix("<"))
      (id, tags)
    }

    val idTagPairs = sc.hadoopRDD(
      jobConf,
      classOf[StreamInputFormat],
      classOf[Text],
      classOf[Text],
      100)
      .map(tuple => tuple._1.toString)
      // parse rows into pairs (id, tags)
      .map(row => parseRow(row))
      // it does not make sense to consider posts with less than 1 tag
      .filter(idAndTags => idAndTags._2.length > 1)
      // flatten the map to get pairs of (id, tag), there can be several pairs with same id
      .flatMap(idAndTags => idAndTags._2.map(tag => Pair(idAndTags._1, tag)))
      // cache for further usage
      .persist(StorageLevel.MEMORY_AND_DISK)

    // assign ids to tags
    val idGenerator = new AtomicInteger(0)
    val idsByTag = new mutable.HashMap[String, Int]()
    val tagsById = new mutable.HashMap[Int, String]()
    // collect all known tags into list
    val tags = idTagPairs
      .map(post => post.tag)
      .distinct()
      .collect()
    tags.foreach(tag => {
      val id = idGenerator.incrementAndGet()
      idsByTag.put(tag, id)
      tagsById.put(id, tag)
    })

    // make mappings available on workers
    val broadcastTagsById = sc.broadcast(tagsById)
    val broadcastIdsByTag = sc.broadcast(idsByTag)

    // train model
    val input = idTagPairs.map(pair => new Rating(pair.id, broadcastIdsByTag.value.get(pair.tag).get, 1.0))
    val model = ALS.train(input, 30, 10, 0.01)

    def formatRating(rating: Double): String = f"$rating%1.3f"


    // predict tags for one post
    def predict(id: Int, numberOfSuggestedTags: Int): Array[(String, String)] = {

      // input for model is (given postId -> tagId) rdd
      val predictionInput = sc.parallelize(idsByTag.values.map(tagId => (id, tagId)).toSeq)

      model
        .predict(predictionInput)
        // get top predicted tags with highest rating
        .top(numberOfSuggestedTags)(Ordering.by(r => r.rating))
        // convert tag id back to text and beautify rating value
        .map(rating => (tagsById.get(rating.product).get, formatRating(rating.rating)))
    }
    // example of usage
    println("10 most suitable tags for 25th post:")
    predict(25, 10).foreach(println)


    // predict tags for several posts at one go
    def predictBatch(ids: List[Int], numberOfSuggestedTags: Int): Array[(Int, Seq[(String, String)])] = {

      // input for model is (given postId -> tagId) rdd
      val predictionInput = sc.parallelize(
        // for each tag
        idsByTag.values
          .flatMap(tagId =>
            // for each input post id
            ids.map(id =>
              // get (given postId -> tagId) pair
              (id, tagId)))
          .toSeq)

      model
        .predict(predictionInput)
        // to reduce number of records
        .filter(rating => rating.rating > 0.5)
        // group by post
        .groupBy(rating => rating.user)
        // for each post take only N best tags
        .mapValues(ratings => ratings.toSeq
          .sortWith((r1, r2) => r1.rating > r2.rating)
          .take(numberOfSuggestedTags)
          // convert tag id back to text and beautify rating value
          .map(rating => (broadcastTagsById.value.get(rating.product).get, formatRating(rating.rating))))
        .collect()
    }
    // example of usage
    println("10 most suitable tags for 5 posts:")
    predictBatch(List(25, 9, 59, 134, 48), 10).foreach(println)
  }

  case class Pair(id: Int, tag: String)

}
