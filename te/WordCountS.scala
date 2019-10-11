package te

import dao.RecUserImpl
import daomain.RecUser
import kafka.serializer.StringDecoder
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import te.TestStreaming._

object WordCountS {
  def main(args: Array[String]): Unit = {


    /*
     val conf=new SparkConf().setAppName("wordcount")
       .set("spark.streaming.kafka.maxRatePerPartition","100")
       .setMaster("local[*]")
     val ssc=new StreamingContext(conf,Seconds(3))
     ssc.sparkContext.setLogLevel("ERROR")
    // ssc.checkpoint("hdfs://master:9000/chk")
     val zkGroup="192.168.249.10:2181"
     val topics=Map("mkl"->2)
     val stream= KafkaUtils.createStream(ssc,"192.168.249.10:2181","zhans",topics)
      stream.map(_._2).foreachRDD(rdd=>rdd.foreach(println(_)))

     ssc.start()
     ssc.awaitTermination()
     */
  }
}
