package streaming


import java.util.{Properties, Random}

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable.ArrayBuffer
// 写了一个程序来模拟kafka生产者，将数据写入kafka之中
object KafkaProducer {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("trainingModel")
    conf.registerKryoClasses(
      Array(classOf[ProducerRecord[String,String]],classOf[KafkaProducer[String,String]]))
    val spark=SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    val num = spark.sql("select user_id from sl.ratings").count()
    var percent=0.6
    val trainNum=Math.floor(num*percent)
    val trainDf=spark.sql("select * from sl.trainingdata limit "+ trainNum.toInt)
    val prop=new Properties()
    prop.put("bootstrap.servers", "192.168.249.10:9092,192.168.249.11:9092")  // kafka的服务器集群的地址
    prop.put("acks", "all")  // 对于向kafka 生产者发送消息 如何确认是成功的模式的设置
    prop.put("retries", "1")  //向broker发送失败重试的次数
    prop.put("batch.size", "16384") // 按照批次进行发送 发送的数据批次的大小
    prop.put("linger.ms", "1") // 吞吐量和延时性能的设置 如果设置为0的话，就是表示数据不会再缓冲区停留，可能会造成小batch的IO
    prop.put("buffer.memory", "33554432") // 缓冲区的大小 kafka生产者在发送消息的时候会开辟一个缓冲区，然后生产者会启动一个线程进行消息的发送
    prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")  // 发送的消息的key 的序列化器
    prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer") // 发送消息的value的序列化器
    val topic="miu"
    val broadTopic=spark.sparkContext.broadcast(topic)

    trainDf.rdd.map(data=>{
      val rd=new Random()
      (rd.nextInt(10)+"",data.getInt(0)+"_"+data.getInt(1)+"_"+data.getDouble(2))
    }).foreachPartition(
      data=>{
          val bTopic=broadTopic.value
          val producer = new KafkaProducer[String,String](prop)
          val iterator=data.toIterator
          var tmp:(String,String) = null
          while(iterator.hasNext){
             tmp = iterator.next()
             println(tmp._1)
             producer.send(new ProducerRecord[String,String](bTopic,tmp._1,tmp._2))
          }
      }
    )
  trainDf.write.mode("overwrite").saveAsTable("sl.trainUsers")
  }
}
