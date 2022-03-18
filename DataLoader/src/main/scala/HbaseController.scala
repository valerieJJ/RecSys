package scala

import org.apache.hadoop.hbase.client.{ConnectionFactory, Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import scala.HotMoviesRec.MONGODB_MOVIE_COLLECTION


object HbaseController {
  val spconf = new SparkConf().setAppName("test-hbase").setMaster("local[*]")
  val sparkSess: SparkSession = SparkSession.builder().config(spconf).getOrCreate()
//  val sc = new SparkContext(spconf)

  val hbconf = HBaseConfiguration.create()
  hbconf.set("hbase.zookeeper.property.clientPort", "2181")
  hbconf.set("hbase.zookeeper.quorum", "master,hadoop0")
  hbconf.set(TableInputFormat.INPUT_TABLE, "cora")

  val mongoURI = "mongodb://localhost:27017/MovieDB"
  val connection = ConnectionFactory.createConnection(hbconf);
  val admin = connection.getAdmin();
  import sparkSess.implicits._

  def movieData(): Unit ={

    val movieDF = sparkSess.read
      .option("uri", mongoURI)
      .option("collection", MONGODB_MOVIE_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Movie]
      .toDF()

    movieDF.createOrReplaceTempView("movieee")
    val movie_moviedf = sparkSess.sql("select mid, count(mid) as count " +
      "from movieee " +
      "group by mid " +
      "limit(3)"
    )
    println("movie_moviedf: ")
    movie_moviedf.rdd.foreach(println)
  }

  def scanDataFromHTable(tableName:String,columnFamily: String, column: String) = {
    //    val TabName = TableName.valueOf("cora")
    val table = connection.getTable(TableName.valueOf(tableName))
    val scan = new Scan()

    scan.addFamily(columnFamily.getBytes()) //添加列簇名称
    val scanner = table.getScanner(scan) //从table中抓取数据来scan
    var result = scanner.next()
    //数据不为空时输出数据
    while (result != null) {
      println(s"rowkey:${Bytes.toString(result.getRow)},列簇:${columnFamily}:${column},value:${Bytes.toString(result.getValue(Bytes.toBytes(columnFamily), Bytes.toBytes(column)))}")
      result = scanner.next()
    }
    //通过scan取完数据后，记得要关闭ResultScanner，否则RegionServer可能会出现问题(对应的Server资源无法释放)
    scanner.close()
  }


  def main(args: Array[String]): Unit = {
    val tables = admin.listTables()
    tables.foreach(println)

    movieData()

    val hbaseRDD = sparkSess.sparkContext.newAPIHadoopRDD(hbconf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result]
    )
    hbaseRDD.foreach{ case (_, result) =>
      val key = Bytes.toString(result.getRow)
      val a_name = Bytes.toString(result.getValue("author".getBytes,"name".getBytes))
      val a_age = Bytes.toString(result.getValue("author".getBytes,"age".getBytes))
      val a_institution = Bytes.toString(result.getValue("author".getBytes,"institution".getBytes))
      val p_title = Bytes.toString(result.getValue("paper".getBytes,"title".getBytes))
      val p_conference = Bytes.toString(result.getValue("paper".getBytes,"conference".getBytes))
      println("\n*** data row-authorId:"+ key
        +", name:" + a_name
        + ", age:" + a_age
        + ", institution:" + a_institution
        + "; paper-title:" + p_title
        + ", paper-conference:" + p_conference
        + "\n"
      )
    }

//    // write to hbase
//    val table = connection.getTable(TableName.valueOf("cora"))
//    val p = new Put(Bytes.toBytes("005"));
//    p.add(Bytes.toBytes("author"), Bytes.toBytes("name"), Bytes.toBytes("billie"));
//    p.add(Bytes.toBytes("author"), Bytes.toBytes("age"), Bytes.toBytes("23"));
//    p.add(Bytes.toBytes("author"), Bytes.toBytes("institution"), Bytes.toBytes("California"));
//    p.add(Bytes.toBytes("paper"), Bytes.toBytes("title"), Bytes.toBytes("paper6"));
//    p.add(Bytes.toBytes("paper"), Bytes.toBytes("conference"), Bytes.toBytes("WWW"));
//    table.put(p)
//    println("new date added!")


    admin.close()


  }

}
