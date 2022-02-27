package scala

import com.mongodb.spark.MongoSpark
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}


object ReadMongo {

  // 定义表名
  val MONGODB_MOVIE_COLLECTION = "Movie"
  val MONGODB_RATING_COLLECTION = "Rating"

  //统计的表的名称
  val RATE_MORE_MOVIES = "RateMoreMovies"
  val RATE_MORE_RECENTLY_MOVIES = "RateMoreRecentlyMovies"
  val AVERAGE_MOVIES = "AverageMovies"
  val GENRES_TOP_MOVIES = "GenresTopMovies"

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()
    val coll_name = "RateMoreMovies"
    val uri: String = "mongodb://localhost:27017/MovieDB."+coll_name
    //"mongodb://username:password@mongodb-host:port/database.table"
    sparkConf.setMaster("local[*]").setAppName(this.getClass.getName)
    sparkConf.set("spark.mongodb.input.uri", uri)

    val sparkSess: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    sparkSess.sparkContext.setLogLevel("warn")

    import sparkSess.implicits._
    implicit val mongoConfig = MongoConfig("mongodb://localhost:27017/MovieDB","MovieDB")

    val schema = StructType(
      List(
        StructField("mid", IntegerType),
        StructField("count", IntegerType)
//        StructField("name", StringType),
//        StructField("directors", StringType)
      )
    )
    val movieDF = sparkSess.read
      .format("com.mongodb.spark.sql")
      .schema(schema).load()
//    val movieDF: DataFrame = MongoSpark.load(sparkSess)
//    tb_test.show()

    val rdd = MongoSpark.load(sparkSess)
    val rdd2 = rdd.filter($"count">3)

    movieDF.createOrReplaceTempView("MovieVO")
    val query = sparkSess.sql("select mid, count from movieVO")
    query.show()


//    val movieDF = sparkSess.read
//      .option("uri", "mongodb://localhost:27017/MovieDB")
//      .option("collection", MONGODB_MOVIE_COLLECTION)
//      .format("com.mongodb.spark.sql")
//      .load()
//      .as[Movie]
//      .toDF()

    sparkSess.close()
  }

  def save2MongoDB(df:DataFrame, coll_name:String)(implicit mongoConfig: MongoConfig):Unit={
    df.write
      .option("uri", mongoConfig.uri)
      .option("collection", coll_name)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
  }
}