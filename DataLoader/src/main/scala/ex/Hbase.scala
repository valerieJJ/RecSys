package scala.ex

object Hbase {


  //  val spconf = new SparkConf().setAppName("test-hbase").setMaster("local[*]")
  //  val sc = new SparkContext(spconf)

  val hbconf = HBaseConfiguration.create()
  hbconf.set("hbase.zookeeper.property.clientPort", "2181")
  hbconf.set("hbase.zookeeper.quorum", "master,hadoop0")
  hbconf.set(TableInputFormat.INPUT_TABLE, "cora")

  val connection = ConnectionFactory.createConnection(hbconf);
  val admin = connection.getAdmin();


  def scanDataFromHTable(tableName:String,columnFamily: String, column: String): String = {
    val table = connection.getTable(TableName.valueOf(tableName))
    val scan = new Scan()

    scan.addFamily(columnFamily.getBytes()) //添加列簇名称
    val scanner = table.getScanner(scan) //从table中抓取数据来scan
    var result = scanner.next()
    val tabInfo = new StringBuilder
    //数据不为空时输出数据
    while (result != null) {
      println(s"rowkey:${Bytes.toString(result.getRow)},列簇:${columnFamily}:${column},value:" +
        s"${Bytes.toString(result.getValue(Bytes.toBytes(columnFamily), Bytes.toBytes(column)))}")
      val tmp = s"rowkey:${Bytes.toString(result.getRow)},列簇:${columnFamily}:${column}," + s"value:${Bytes.toString(result.getValue(Bytes.toBytes(columnFamily), Bytes.toBytes(column)))}"
      tabInfo ++= tmp
      tabInfo += '\n'
      result = scanner.next()
    }

    //通过scan取完数据后，记得要关闭ResultScanner，否则RegionServer可能会出现问题(对应的Server资源无法释放)
    scanner.close()
    //    println("tabInfo"+tabInfo.toString)
    return tabInfo.toString()
  }

  def scan_run(): String ={

    val tables = admin.listTables()
    tables.foreach(println)

    //  val coraTable = new HTable(hbconf,"cora")
    //    val coraTable = connection.getTable(TableName.valueOf( Bytes.toBytes("cora") ) )
    //
    val tabName = TableName.valueOf("cora")
    val getTableContent = scanDataFromHTable(tableName = "cora", columnFamily = "author", column = "name")
    return getTableContent
  }

  def main(args: Array[String]): Unit = {
    val tableContent = scan_run()
    println("\nHbase Table Content: \n"+tableContent)

  }
}
