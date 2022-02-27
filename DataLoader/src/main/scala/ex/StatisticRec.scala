package scala
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

//case class Movie(mid:Int, name:String, descri: String, timelong: String, issue: String,
//                 shoot: String, language:String, genres:String, actors: String, directors: String)
case class Rating(uid: Int, mid:Int, score:Double, timestamp:Int)
case class BasicRecommandation(mid:Int, score:Double)
case class GenresRecommendation(genres:String, recs:Seq[BasicRecommandation])

case class MongoConfig(uri:String, db:String)

object StatisticRec {

  // 定义表名
  val MONGODB_MOVIE_COLLECTION = "Movie"
  val MONGODB_RATING_COLLECTION = "Rating"

  //统计的表的名称
  val RATE_MORE_MOVIES = "RateMoreMovies"
  val RATE_MORE_RECENTLY_MOVIES = "RateMoreRecentlyMovies"
  val AVERAGE_MOVIES = "AverageMovies"
  val GENRES_TOP_MOVIES = "GenresTopMovies"

  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores"-> "local[*]",
      "mongo.uri"->"mongodb://localhost:27017/MovieDB",
      "mongo.db"->"MovieDB"
    )

    val sparkConf: SparkConf = new SparkConf()
    sparkConf.setMaster(config("spark.cores")).setAppName(this.getClass.getName)

    val sparkSess: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    val ratingDF = sparkSess.read
      .option("uri", "mongodb://localhost:27017/MovieDB",)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Rating]
      .toDF()
    println("ratingDF.head()")
    println(ratingDF.head())

    val movieDF = sparkSess.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_MOVIE_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Movie]
      .toDF()

    // 创建临时表
    ratingDF.createOrReplaceTempView("Ratings")
    // 统计热门，历史评论数量越多越热门
    val moreRates_moviedf = sparkSess.sql("select mid, count(mid) as count " +
      "from ratings " +
      "group by mid")
    saveDF2Mongodb(moreRates_moviedf, RATE_MORE_MOVIES)
  }

  def saveDF2Mongodb(df:DataFrame, coll_name:String)(implicit mongoConfig: MongoConfig):Unit={
    df.write
      .option("uri", mongoConfig.uri)
      .option("collection", coll_name)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
  }

}
