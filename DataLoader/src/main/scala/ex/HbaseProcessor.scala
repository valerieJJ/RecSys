package scala.ex

import java.io.IOException

import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put, ResultScanner, Scan, Table}
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ListBuffer
/**
 * 从hbase中增删改查数据
 *
 */
object HbaseUtils {

  val hbconf = HBaseConfiguration.create()
  hbconf.set("hbase.zookeeper.property.clientPort", "2181")
  hbconf.set("hbase.zookeeper.quorum", "master,hadoop0")
  val connection = ConnectionFactory.createConnection(hbconf);
  val admin = connection.getAdmin();

  def isExists(tableName: String):Boolean ={
    var result=false
    val tName = TableName.valueOf(tableName)
    if(admin.tableExists(tName)) {
      result=true
    }
    result
  }
  //创建一个hbase表
  def createTable(tableName: String, columnFamilys: Array[String]) = {
    //操作的表名
    val tName = TableName.valueOf(tableName)
    //当表不存在的时候创建Hbase表
    if (!admin.tableExists(tName)) {
      //创建Hbase表模式
      val descriptor = new HTableDescriptor(tName)
      //创建列簇i
      for (columnFamily <- columnFamilys) {
        descriptor.addFamily(new HColumnDescriptor(columnFamily))
      }
      //创建表
      admin.createTable(descriptor)
      println("create successful!!")
    }
  }

  def dropTable(tableName: String):Unit={
    admin.disableTable(TableName.valueOf(tableName))
    admin.deleteTable(TableName.valueOf(tableName))
    println("drop successful!!")
  }
  //向hbase表中插入数据
  //put 'sk:test1','1','i:name','Luck2'
  def insertTable(tableName:String,rowkey: String, columnFamily: String, column: String, value: String) = {
    val table = connection.getTable(TableName.valueOf(tableName))
    //准备key 的数据
    val puts = new Put(rowkey.getBytes())
    //添加列簇名,字段名,字段值value
    puts.addColumn(columnFamily.getBytes(), column.getBytes(), value.getBytes())
    //把数据插入到tbale中
    table.put(puts)
    println("insert successful!!")
  }

  //获取hbase表中的数据
  //scan 'sk:test1'
  def scanDataFromHTable(tableName:String,columnFamily: String, column: String) = {
    val table = connection.getTable(TableName.valueOf(tableName))
    //定义scan对象
    val scan = new Scan()
    //添加列簇名称
    scan.addFamily(columnFamily.getBytes())
    //从table中抓取数据来scan
    val scanner = table.getScanner(scan)
    var result = scanner.next()
    //数据不为空时输出数据
    while (result != null) {
      println(s"rowkey:${Bytes.toString(result.getRow)},列簇:${columnFamily}:${column},value:${Bytes.toString(result.getValue(Bytes.toBytes(columnFamily), Bytes.toBytes(column)))}")
      result = scanner.next()
    }
    //通过scan取完数据后，记得要关闭ResultScanner，否则RegionServer可能会出现问题(对应的Server资源无法释放)
    scanner.close()
  }
  //只传表名得到全表数据
  def getAllData(tableName: String): ListBuffer[String] = {
    var table: Table = null
    val list=new ListBuffer[String]
    try {
      table = connection.getTable(TableName.valueOf(tableName))
      val results: ResultScanner = table.getScanner(new Scan)
      for (result <- results) {
        for (cell <- result.rawCells) {
          val row: String = Bytes.toString(cell.getRowArray, cell.getRowOffset, cell.getRowLength)
          val family: String = Bytes.toString(cell.getFamilyArray, cell.getFamilyOffset, cell.getFamilyLength)
          val colName: String = Bytes.toString(cell.getQualifierArray, cell.getQualifierOffset, cell.getQualifierLength)
          val value: String = Bytes.toString(cell.getValueArray, cell.getValueOffset, cell.getValueLength)
          val context: String = "rowkey:" + row +","+ "列族:" + family +","+ "列:" + colName +","+ "值:" + value
          list+=context
        }
      }
      results.close()
    } catch {
      case e: IOException =>
        e.printStackTrace()
    }
    list
  }

  //删除某条记录
  //delete 'sk:test1','1','i:name'
  def deleteRecord(tableName:String,rowkey: String, columnFamily: String, column: String) = {
    val table = connection.getTable(TableName.valueOf(tableName))

    val info = new Delete(Bytes.toBytes(rowkey))
    info.addColumn(columnFamily.getBytes(), column.getBytes())
    table.delete(info)
    println("delete successful!!")
  }
  // 关闭 connection 连接
  def close()={
    if (connection!=null){
      try{
        connection.close()
        println("关闭成功!")
      }catch{
        case e:IOException => println("关闭失败!")
      }
    }
  }

  def main(args: Array[String]): Unit = {
    /** Movie 数据集
   * 260                                         电影ID，mid
   * Star Wars: Episode IV - A New Hope (1977)   电影名称，name
   * Princess Leia is captured and held hostage  详情描述，descri
   * 121 minutes                                 时长，timelong
   * September 21, 2004                          发行时间，issue
   * 1977                                        拍摄时间，shoot
   * English                                     语言，language
   * Action|Adventure|Sci-Fi                     类型，genres
   * Mark Hamill|Harrison Ford|Carrie Fisher     演员表，actors
   * George Lucas                                导演，directors
   */
    var arr = Array("mid","name", "descri","timelong","issue","language","directors")
    //arr(0)="info1"
    var col: String = "mid"
//    createTable("MovieData", arr)
    //insertTable("user2","1","info1","name","lyh")
    //scanDataFromHTable("user2","info1","name")
    //deleteRecord("user2","1","info1","name")
//    dropTable("user2")
//    println(isExists("user2"))
    close()
  }
}