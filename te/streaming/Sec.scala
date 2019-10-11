/*
package te.streaming

import dao.RecUserImpl
import daomain.RecUser
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.I0Itec.zkclient.ZkClient
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils
import streaming.{BroadMovieIds, TrainData, TrainModel}
import streaming.SparkConsumerV2._


object Sec {
// sparkStreaming direct 消费的代码
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf()
      .setAppName("sparkStreaming direct consumer")
      .set("spark.streaming.kafka.maxRatePerPartition","100")
      .set("spark.streaming.stopGracefullyOnShutdown","true")  //优雅地关闭spark streaming
      .setMaster("local[*]")
    val spark=SparkSession.builder().config(conf).getOrCreate()
    // 对driver 进行容错
    val ssc=new StreamingContext(spark.sparkContext,Seconds(3))
    val sc=ssc.sparkContext
    ssc.sparkContext.setLogLevel("ERROR")
    val topic="miu"
    val topics=Array(topic).toSet
    val consumerGroup="mms"
    val brokerList="192.168.249.10:9092,192.168.249.11:9092"
    val zkHost="192.168.249.10:2181,192.168.249.11:2181"

    val kafkaPrams=Map[String,String](
      "metadata.broker.list"->brokerList,
      "group.id"->consumerGroup,
      "zookeeper.connect"->zkHost,
      "auto.offset.reset"->"smallest"
    )
    val kafkaStream=KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaPrams,topics)
     kafkaStream.map(_._2).foreachRDD(rdd=>{
       rdd.foreach(println(_))
     })
    ssc.start()
    ssc.awaitTermination()
  }
}
*/