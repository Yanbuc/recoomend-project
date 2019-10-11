package te.streaming

import kafka.serializer.StringDecoder
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

// sparkStreaming receiver消费模式
object SparkStreamingReceiver {
  def main(args: Array[String]): Unit = {
/*
           val conf = new SparkConf().setMaster("local[2]").setAppName("StreamingKafkaApp01")
           val ssc = new StreamingContext(conf,Seconds(5))

           val topics = "something".split(",").map((_,1)).toMap
           val lines = KafkaUtils.createStream(ssc,"192.168.249.10:2181","huluwa_group",topics)
           lines.print()

           ssc.start()
           ssc.awaitTermination()
*/

         val conf=new SparkConf().setAppName("sparksss").setMaster("local[2]")
         conf.set("spark.streaming.receiver.writeAheadLog.enable","true")
         val ssc=new StreamingContext(conf,Seconds(3))
          ssc.sparkContext.setCheckpointDir("hdfs://master:9000")
         val zkGroup="192.168.249.10:2181"
          ssc.sparkContext.setLogLevel("ERROR")
          val topics = "something".split(",").map((_,1)).toMap
         val lines = KafkaUtils.createStream(ssc,zkGroup,"one",topics)
         lines.foreachRDD(rdd=>{
            println(rdd.count())
         })
         lines.print()
         ssc.start()
         ssc.awaitTermination()


  }

}
