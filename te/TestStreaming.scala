package te

import dao.RecUserImpl
import daomain.RecUser
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.utils.ZKGroupTopicDirs
import org.I0Itec.zkclient.ZkClient
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.{InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}


object TestStreaming {

  def  getKafkaDirectStream(ssc:StreamingContext,kafkaParams:Map[String,String],topic:String,
                            zkClient: ZkClient,children:Int,consumerOffsetDir:String):InputDStream[(String,String)]={
    println("create intput stream")
    var kafkaStream:InputDStream[(String,String)]=null
    val topics=Array(topic).toSet[String]
    if(children>0){
      var partitionOffset = Map[TopicAndPartition, Long]()
      for(i<- 0 until children){
        val offset:String=zkClient.readData[String](consumerOffsetDir+"/"+i)
        val tp=TopicAndPartition(topic,i)
        partitionOffset += (tp->offset.toLong)
      }
      val messageHandler =(mmd:MessageAndMetadata[String, String] )=>(mmd.topic,mmd.message())
      kafkaStream = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder,(String,String)](
        ssc,kafkaParams, partitionOffset,messageHandler)
    }else{

      val messageHandler =(mmd:MessageAndMetadata[String, String] )=>(mmd.topic,mmd.message())
      //kafkaStream = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder,(String,String)](
      // ssc,kafkaParams, partitionOffset,messageHandler)
      var kp=kafkaParams
      kp += ("auto.offset.reset"->"smallest")
      kafkaStream=KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kp,topics)
    }
    println("yes create is ok")
    kafkaStream
  }

  def getZkClient(zkHost:String):ZkClient={
    val zkCLient=new ZkClient(zkHost)
    zkCLient
  }
  def getZkTopicDirs(group: String,topic:String):ZKGroupTopicDirs={
    val zkGroupDirs=new ZKGroupTopicDirs(group,topic)
    zkGroupDirs
  }

  def getKafkaParams(zkHost:String,brokerList:String,consumerGroup:String):Map[String,String]={
    val kafkaPrams=Map[String,String](
      "metadata.broker.list"->brokerList,
      "group.id"->consumerGroup,
      "zookeeper.connect"->zkHost
    )
    kafkaPrams
  }

  def getTopicChildrenNum(zkClient: ZkClient,consumerOffsetDirs:String):Int={
    val children = zkClient.countChildren(consumerOffsetDirs)
    children
  }

  def streamingContextFactory():StreamingContext= {
    val conf=new SparkConf()
      .setAppName("sparkStreaming direct consumer")
      .set("spark.streaming.kafka.maxRatePerPartition","100")
      .registerKryoClasses(Array(classOf[MatrixFactorizationModel],classOf[RecUser],classOf[RecUserImpl],classOf[org.I0Itec.zkclient.ZkClient]))
    val spark=SparkSession.builder().config(conf).getOrCreate()
    // 对driver 进行容错
    val checkPointpath="hdfs://master:9000/movie/cmp"
    val ssc=new StreamingContext(spark.sparkContext,Seconds(3))
    ssc.checkpoint(checkPointpath)
    val zkHost="192.168.249.10:2181,192.168.249.11:2181"
    val zkClient=getZkClient(zkHost)
    ssc.sparkContext.setLogLevel("ERROR")
    val topic="miu"
    val topics=Array(topic).toSet
    val consumerGroup="syu"
    val brokerList="192.168.249.10:9092,192.168.249.11:9092"
    val zkGroupTopicDirs=getZkTopicDirs(consumerGroup,topic)
    val consumerOffsetDir=zkGroupTopicDirs.consumerOffsetDir
    val kafkaParams=getKafkaParams(zkHost,brokerList,consumerGroup)
    val children=getTopicChildrenNum(zkClient,consumerOffsetDir)
    val kafkaStream=getKafkaDirectStream(ssc,kafkaParams,topic,zkClient,children,consumerOffsetDir)
    kafkaStream.map(line=>{
      line._2
    }).foreachRDD(rdd=>{
      val zk=getZkClient("192.168.249.10:2181,192.168.249.11:2181")
      println(zk.toString)
    })
    ssc
  }

  def main(args: Array[String]): Unit = {
    val checkPointpath="hdfs://master:9000/movie/cmp"
    val ssc=StreamingContext.getOrCreate(checkPointpath,streamingContextFactory _)
    ssc.start()
    ssc.awaitTermination()
  }
}
