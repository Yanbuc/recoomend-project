package streaming

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}


object SparkDirectConsumer {
   // 获得topic 的offset dir
  def getTopicDir(group:String,topic:String):ZKGroupTopicDirs={
    val topicDirs = new ZKGroupTopicDirs(group, topic)
    topicDirs
  }

  def getKafkaStream(ssc:StreamingContext,
                     kafkaParams:Map[String,String],
                     topic:String,zkClient: ZkClient,zkTopicPath:String):InputDStream[(String, String)]={
    val children=zkClient.countChildren(zkTopicPath)
    var kafkaStream : InputDStream[(String, String)] = null
    var fromOffsets: Map[TopicAndPartition, Long] = Map()
    val topics=Array(topic).toSet
    if(children>0){
      for(i <- 0 until children){
        val offset: String = zkClient.readData[String](zkTopicPath+"/"+i)
        val tp=TopicAndPartition(topic,i)
        fromOffsets += (tp->offset.toLong)
      }
      val messageHandler = (mmd : MessageAndMetadata[String, String]) => (mmd.topic, mmd.message())
      kafkaStream=KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, fromOffsets, messageHandler)
    }else{
      kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
    }
    kafkaStream
  }

  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("sparkStreaming direct consumer").setMaster("local")
    val ssc=new StreamingContext(conf,Seconds(3))
    ssc.sparkContext.setLogLevel("ERROR")
    val topics=Array("tk").toSet[String]
    val topic : String = "tk" //消费的 topic 名字
    val consumerGroup="tpp"
    val zkHost="192.168.249.10:2181,192.168.249.11:2181"
    val brokerList="192.168.249.10:9092,192.168.249.11:9092"

    val topicDirs = getTopicDir(consumerGroup,topic)
    val zkTopicPath:String = s"${topicDirs.consumerOffsetDir}"
    val zkClient = new ZkClient(zkHost)
    // 查看topic 下面是否存在分区
    val children = zkClient.countChildren(zkTopicPath)
    var kafkaStream : InputDStream[(String, String)] = null // 消费kafka的流
    var fromOffsets: Map[TopicAndPartition, Long] = Map() // 存放topic分区下方的offset数据的东西
    val kafkaParams=Map[String,String](
      "metadata.broker.list"->brokerList,
      "group.id"->consumerGroup,
      "zookeeper.connect"->zkHost)

    kafkaStream=getKafkaStream(ssc,kafkaParams,topic,zkClient,zkTopicPath)
    var offsetRanges = Array[OffsetRange]()

    kafkaStream.transform{ rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      //得到该 rdd 对应 kafka 的消息的 offset
      rdd
    }.map(msg => msg._2).foreachRDD { rdd =>
      for (o <- offsetRanges) {
        val zkPath = s"${topicDirs.consumerOffsetDir}/${o.partition}"
        ZkUtils.updatePersistentPath(zkClient, zkPath, o.fromOffset.toString)
        //将该 partition 的 offset 保存到 zookeeper
        println(s"@@@@@@ topic  ${o.topic}  partition ${o.partition}  fromoffset ${o.fromOffset}  untiloffset ${o.untilOffset} #######")
      }
      rdd.foreachPartition(
        message => {
          while(message.hasNext) {
            println(s"@^_^@   [" + message.next() + "] @^_^@")
          }
        }
      )
    }

    ssc.start()
    ssc.awaitTermination()

  }
}


/*
if (children > 0) {
  //如果保存过 offset，这里更好的做法，还应该和
  // kafka 上最小的 offset 做对比，不然会报 OutOfRange 的错误
  for (i <- 0 until children) {
    val partitionOffset = zkClient.readData[String](s"${topicDirs.consumerOffsetDir}/${i}")
    val tp = TopicAndPartition(topic, i)
    fromOffsets += (tp -> partitionOffset.toLong)  //将不同 partition 对应的 offset 增加到 fromOffsets 中
    // logInfo("@@@@@@ topic[" + topic + "] partition[" + i + "] offset[" + partitionOffset + "] @@@@@@")
  }
  val messageHandler = (mmd : MessageAndMetadata[String, String]) => (mmd.topic, mmd.message())
  //这个会将 kafka 的消息进行 transform，最终 kafka 的数据都会变成 (topic_name, message) 这样的 tuple
  kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, fromOffsets, messageHandler)
}
else {
  kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
  //如果未保存，根据 kafkaParam 的配置使用最新或者最旧的 offset
}
*/