
import java.net.InetAddress

import DataLoader.{MONGODB_MOVIE_COLLECTION, MONGODB_RATING_COLLECTION, MONGODB_TAG_COLLECTION, MOVIE_DATA_PATH, TAG_DATA_PATH}
import org.apache.spark
import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.ReadConfig
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.transport.client.PreBuiltTransportClient

object MongodbConnector {

  // 定义常量
  val MOVIE_DATA_PATH = "movie-data/movies.csv"
  val RATING_DATA_PATH = "movie-data/ratings.csv"
  val TAG_DATA_PATH = "movie-data/tags.csv"

  val MONGODB_MOVIE_COLLECTION = "Movie"
  val MONGODB_RATING_COLLECTION = "Rating"
  val MONGODB_TAG_COLLECTION = "Tag"
  val ES_MOVIE_INDEX = "Movie"

  //创建一个Spark config
//  val sparkConf = new SparkConf().setMaster("local[*]").setAppName("MongoDB")
  //创建spark session
  //  val spark = SparkSession.builder().config(sparkConf).getOrCreate()
//  val spark = SparkSession.builder()
//    .master("local[*]")
//    .appName("MongoSpark")
//    .config("spark.mongodb.input.uri", "mongodb://localhost/MovieDB")
//    .config("spark.mongodb.output.uri", "mongodb://localhost/MovieDB")
//    .getOrCreate()
//  var sc = spark.sparkContext

  def main(args: Array[String]): Unit = {
//    readMongo()

    val sparkConf: SparkConf = new SparkConf()
    val uri: String = "mongodb://localhost/MovieDB.recommend"
    sparkConf.setMaster("local[*]").setAppName(this.getClass.getName)
    sparkConf.set("spark.mongodb.input.uri",uri)

    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    sparkSession.sparkContext.setLogLevel("warn")

    val tb_test: DataFrame = MongoSpark.load(sparkSession)
    tb_test.show()

    sparkSession.close()

//    var data = getDF()
//    var movieDF = data(0)
//    var ratingDF = data(1)
//    var tagDF = data(2)

//    println(movieDF.head())
//    println(ratingDF.head())
//    println(tagDF.head())

  }

//  def readMongo():Unit={
//    val readConfig = ReadConfig(Map("collection" -> "spark", "readPreference.name" -> "secondaryPreferred"), Some(ReadConfig(sc)))
//    val customRdd = MongoSpark.load(sc, readConfig)
//    println(customRdd.count)
//    println(customRdd.first.toJson)
//  }
//
//  def getDF(): Array[DataFrame] = {
//    import spark.implicits._
//    //加载数据
//    val movieRDD = spark.sparkContext.textFile(MOVIE_DATA_PATH)
//    val movieDF = movieRDD.map(
//      item => {
//        val attr = item.split("\\^")
//        Movie(attr(0).toInt, attr(1).trim, attr(2).trim, attr(3).trim, attr(4).trim, attr(5).trim, attr(6).trim, attr(7).trim, attr(8).trim, attr(9).trim)
//      }
//    ).toDF()
//
//    //加载数据
//    val RatingRDD = spark.sparkContext.textFile(RATING_DATA_PATH)
//    val ratingDF = RatingRDD.map(item => {
//      val attr = item.split("\\^")
//      Rating(attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toInt)
//    }).toDF()
//
//    val tagRDD = spark.sparkContext.textFile(TAG_DATA_PATH)
//    //将tagRDD装换为DataFrame
//    val tagDF = tagRDD.map(item => {
//      val attr = item.split(",")
//      Tag(attr(0).toInt,attr(1).toInt,attr(2).trim,attr(3).toInt)
//    }).toDF()
//
//    var data = Array(movieDF,ratingDF,tagDF)
//    return data
//  }


}
