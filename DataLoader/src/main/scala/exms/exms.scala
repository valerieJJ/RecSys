package scala.exms

import org.apache.spark.{SparkConf, SparkContext}

object exms {
  def main(args: Array[String]): Unit = {
    val sparkConfig = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getName)
    val sc = new SparkContext(sparkConfig)

    val a = sc.parallelize(Array(1.1,1.2,1.3))
    val b = sc.parallelize(Array(2.1,2.2,2.3))
    val c = a.cartesian(b).collect()
    c.foreach(print)
  }
}
