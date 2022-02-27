package scala

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.bson.conversions.Bson

case class Movie(mid:Int, name:String, descri: String, timelong: String, issue: String,
                 shoot: String, language:String, genres:String, actors: String, directors: String)
case class Rating(uid: Int, mid:Int, score:Double, timestamp:Int)
case class MongoConfig(uri:String, db:String)
case class BasicRecommandation(mid:Int, score:Double)
case class GenresRecommendation(genres:String, recs:Seq[BasicRecommandation])

object HotMoviesRec {

  // 定义表名
  val MONGODB_MOVIE_COLLECTION = "Movie"
  val MONGODB_RATING_COLLECTION = "Rating"

  //统计的表的名称
  val RATE_MORE_MOVIES = "RateMoreMovies"
  val RATE_MORE_RECENTLY_MOVIES = "RateMoreRecentlyMovies"
  val MOVIE_AVG_SCORE = "MovieAverageScore"
  val GENRES_TOP_MOVIES = "GenresTopMovies"

  def main(args: Array[String]): Unit = {

    val mongoURI = "mongodb://localhost:27017/MovieDB"
    val dbName = "MovieDB"

    val sparkConf: SparkConf = new SparkConf()
    sparkConf.setMaster("local[*]").setAppName(this.getClass.getName)

    val sparkSess: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    import sparkSess.implicits._

    /**************** 热门推荐  *******************/
    val ratingDF = sparkSess.read
      .option("uri", "mongodb://localhost:27017/MovieDB")
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Rating]
      .toDF()
    hotRec(ratingDF, mongoURI, sparkSess)
    val movieDF = sparkSess.read
      .option("uri", mongoURI)
      .option("collection", MONGODB_MOVIE_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Movie]
      .toDF()

    /**************** 分类推荐，每一类按分数排序  *******************/
    val avgScoreDF = sparkSess.sql("select mid, avg(score) as avgScore " +
                                          "from ratings " +
                                          "group by mid")
    print("best scores ---")
    avgScoreDF.filter($"avgScore">4.5).show()
//    val topScores = avgScoreDF.filter($"avgScore">4.5)
    val topScores = avgScoreDF.filter("avgScore>4.5")
    topScores.show(30)
    println("sort")
    topScores.sort($"avgScore".asc).show(30)

//    saveDF2Mongodb(avgScoreDF, MOVIE_AVG_SCORE, mongoURI)
    val genres = List("Action","Adventure","Animation","Comedy","Crime","Documentary","Drama","Family","Fantasy","Foreign","History","Horror","Music","Mystery"
      ,"Romance","Science","Tv","Thriller","War","Western")

  }

  def hotRec(ratingDF:DataFrame, mongoURI:String, sparkSess: SparkSession):Unit={
    // 创建临时表
    ratingDF.createOrReplaceTempView("Ratings")
    // 统计热门，历史评论数量越多越热门
    val moreRates_moviedf = sparkSess.sql("select mid, count(mid) as count " +
                                                    "from ratings " +
                                                    "group by mid")
    println("moreRates_moviedf.head()")
    println(moreRates_moviedf.head())
    println("done...")

    //    saveDF2Mongodb(moreRates_moviedf, RATE_MORE_MOVIES, mongoURI)
  }
  def saveDF2Mongodb(df:DataFrame, coll_name:String, mongoURI:String):Unit={
    df.write
      .option("uri", mongoURI)
      .option("collection", coll_name)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
  }
}
