package dataloader
import org.apache.spark

import org.apache.spark.SparkConf
import org.apache.spark.{SparkConf, SparkContext, SparkFiles}
import org.apache.spark.sql._
import org.apache.spark.sql.SparkSession
import org.apache.hadoop
object DataProcessor {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("recsys")
    val sc = new SparkContext(conf)

    val textRDD = sc.textFile("hdfs://master:9000/jjProj/words.txt")
    textRDD.flatMap(line=>line.split(" "))
      .map(x=>(x,1))
      .reduceByKey(_+_)
      .collect()
      .foreach(println)

  }
}
