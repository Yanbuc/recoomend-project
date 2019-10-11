package te

import org.apache.spark.{SparkConf, SparkContext}

object TestSpark {
  def main(args: Array[String]): Unit = {
      val conf=new SparkConf().setAppName("sparkConf").setMaster("local")
      val sc= new SparkContext(conf)
  }
}
