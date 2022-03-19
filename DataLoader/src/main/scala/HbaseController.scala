package scala

import org.apache.hadoop.hbase.client.{ConnectionFactory, HTable, Put, Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructField
import org.apache.spark.{SparkConf, SparkContext}

import scala.HotMoviesRec.MONGODB_MOVIE_COLLECTION


object HbaseController {
  val spconf = new SparkConf().setAppName("test-hbase").setMaster("local[*]")
  val sparkSess: SparkSession = SparkSession.builder().config(spconf).getOrCreate()
//  val sc = new SparkContext(spconf)

  val hbconf = HBaseConfiguration.create()
  hbconf.set("hbase.zookeeper.property.clientPort", "2181")
  hbconf.set("hbase.zookeeper.quorum", "master,hadoop0")
//  hbconf.set(TableInputFormat.INPUT_TABLE, "cora")

  val mongoURI = "mongodb://localhost:27017/MovieDB"
  val connection = ConnectionFactory.createConnection(hbconf);
  val admin = connection.getAdmin();
  import sparkSess.implicits._

  def create(columnFamily:List[String], tableName:String): Unit ={
    //创建 hbase 表描述
    val tName = TableName.valueOf(tableName)
    if (!admin.tableExists(tName)) {//若表不存在则创建
      val tname = new HTableDescriptor(tName)
      //添加列族，新建行模式当做列族
      columnFamily.foreach(x=>tname.addFamily(new HColumnDescriptor(x)))
      admin.createTable(tname)
    }
    println("create table successful...")
  }

  def dropTable(tableName: String):Unit={
    admin.disableTable(TableName.valueOf(tableName))
    admin.deleteTable(TableName.valueOf(tableName))
    println("drop table successful...")
  }

  def movieData(): Unit ={

    val movieDF = sparkSess.read
      .option("uri", mongoURI)
      .option("collection", MONGODB_MOVIE_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Movie]
      .toDF()

    movieDF.createOrReplaceTempView("movieee")
    val movie_moviedf = sparkSess.sql("select * " +
      "from movieee"
//      "from movieee " +
//      "group by mid " +
//      "limit(3)"
    )
    println("movie_moviedf: ")
    movie_moviedf.rdd.foreach(println)
    val movieRDD = movie_moviedf.rdd

//    dropTable("MovieDB")
    create(List("info","people","extra"), "MovieDB")

    movieRDD.foreachPartition(x=>{
      val table = connection.getTable(TableName.valueOf("MovieDB"))
//      val table = connection.getTable(TableName.valueOf("MovieDB"))
      val list = new java.util.ArrayList[Put]
      x.foreach(y=>{
        val schemas = y.schema.fieldNames
        val actors = y.getAs[String]("actors")//y.get(1)
        val descri = y.getAs[String]("descri")// y.get(2)
        val _id = y.getAs[Object]("_id")//y.get(0)
        val directors = y.getAs[String]("directors")//y.get(3)
        val genres = y.getAs[String]("genres")//y.get(4)
        val issue = y.getAs[String]("issue")//y.get(5)
        val language = y.getAs[String]("language")//y.get(6)
        val mid = y.getAs[Int]("mid")//y.get(7)
        val name = y.getAs[String]("name")//y.get(8)
        val shoot = y.getAs[String]("shoot")//y.get(9)
        val timelong = y.getAs[String]("timelong")//y.get(10)

        val put = new Put(Bytes.toBytes(String.valueOf(mid)))
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("_id"), Bytes.toBytes(String.valueOf(_id)))
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("mid"), Bytes.toBytes(String.valueOf(mid)))
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("name"), Bytes.toBytes(name))
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("genres"), Bytes.toBytes(genres))
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("issue"), Bytes.toBytes(issue))
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("language"), Bytes.toBytes(language))
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("shoot"), Bytes.toBytes(shoot))
        put.addColumn(Bytes.toBytes("people"),Bytes.toBytes("actors"), Bytes.toBytes(actors))
        put.addColumn(Bytes.toBytes("people"),Bytes.toBytes("directors"), Bytes.toBytes(directors))
        put.addColumn(Bytes.toBytes("extra"),Bytes.toBytes("timelong"), Bytes.toBytes(timelong))
        put.addColumn(Bytes.toBytes("extra"),Bytes.toBytes("descri"), Bytes.toBytes(descri))

        list.add(put)
        println("arr: "+name)
      })
      table.put(list)
      table.close()
    })
    println("movie write to hbase successful")

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


    admin.close()
  }

  def exm(): Unit ={
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
  }

}
