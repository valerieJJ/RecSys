package scala

import org.apache.spark.SparkConf

object MusicProcessor {
  def main(args: Array[String]): Unit = {
    val sparkConfig: SparkConf = new SparkConf()
    sparkConfig.setMaster("local[*]").setAppName(this.getClass.getName)
  }
}
