
package streaming

import java.util
import java.util.Properties

import dao.RecUserImpl
import daomain.RecUser
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.recommendation.{MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ArrayBuffer

object SparkConsumerV2 {

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

  def writeReccommendMovieIdToMysql(sc:SparkContext,data:ArrayBuffer[(Int,String)])={
    val spark= SparkSession.builder().config(sc.getConf).getOrCreate()
    import spark.implicits._
    val value: RDD[(Int, String)] = sc.parallelize(data,2)
    val url="jdbc:mysql://192.168.249.10:3306/movie"
    val tableName="rec_user"
    val properties=new Properties()
    properties.setProperty("user","root")
    properties.setProperty("password","111111")
    properties.setProperty("driver", "com.mysql.jdbc.Driver")
    value.map(data=>{
      new RecUser(data._1,data._2)
    }).foreachPartition(iterator=>{
      val recUserImpl=new RecUserImpl()
      val data: java.util.List[RecUser] = new util.ArrayList[RecUser]()
      while(iterator.hasNext){
        data.add(iterator.next())
      }
      recUserImpl.insertAndUpdate(data)
    })
  }

  // 获取评分数量最多的电影
  def getTopMovie(sc:SparkContext):Array[Int]={
    val spark=SparkSession.builder().config(sc.getConf).enableHiveSupport().getOrCreate()
    val df = spark.sql("select movie_id,count(*) as cnt from sl.ratings group by movie_id order by cnt desc limit 5 ")
    val rows: Array[Row] = df.take(10)
    rows.map(row=>row.getInt(0))
  }

  def getTopMovieFromMysql(sc:SparkContext):Array[Int]={
    val spark=SparkSession.builder().config(sc.getConf).enableHiveSupport().getOrCreate()
    val data:DataFrame= spark.read.format("jdbc")
      .option("url", "jdbc:mysql://192.168.249.10:3306/movie")
      .option("dbtable", "top_movie")
      .option("user", "root")
      .option("password", "111111")
      .load()
    /*
  .option("fetchsize", "1000")
  .option("truncate", "true")
  .option("numPartitions", "6")
  .option("partitionColumn", "UserID")
  .option("lowerBound", "1000")
  .option("upperBound", "6000")
  */
  val moviesId: Array[Int] = data.rdd.map(row=>row.getInt(1)).take(5)
    moviesId
  }


  def getTrainUserIds(sc:SparkContext):DataFrame={
    val spark=SparkSession.builder().config(sc.getConf).enableHiveSupport().getOrCreate()
    val trainDf= spark.sql("select user_id from sl.trainUsers ")
    trainDf
  }
  // 查询文件是否存在
  def fileExists(file:String):Boolean={
    val conf=new Configuration()
    val path=new Path(file)
    val fileSystem=path.getFileSystem(conf)
    fileSystem.exists(path)
  }
  // 关闭sparkStreaming
  def stopStreamingContextByMarkFile(ssc:StreamingContext,file:String)={
    val waitTime=60*1000
    var isStoped=false
    while(!isStoped){
      var flag=ssc.awaitTerminationOrTimeout(waitTime)
      if(flag==false){
        println("wait time out")
        println("file exists "+fileExists(file))
      }
      if(!flag && fileExists(file)){
        isStoped=true
        println("两秒之后关闭 sparkstreaming")
        ssc.stop(true,true)
      }
    }

  }

  def streamingContextFactory():StreamingContext={
    val conf=new SparkConf()
      .setAppName("sparkStreaming direct consumer")
      .set("spark.streaming.kafka.maxRatePerPartition","100")  // 这里是为了限流而使用的配置参数
      .set("spark.streaming.stopGracefullyOnShutdown","true")  //优雅地关闭spark streaming
      .registerKryoClasses(Array(classOf[MatrixFactorizationModel],classOf[RecUser],classOf[RecUserImpl],BroadMovieIds.getClass,TrainData.getClass,TrainModel.getClass))
    val spark=SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    // 对driver 进行容错
    val checkPointpath="hdfs://master:9000/movie/tmp"
    val ssc=new StreamingContext(spark.sparkContext,Seconds(3))
    ssc.checkpoint(checkPointpath)
    val sc=ssc.sparkContext
    ssc.sparkContext.setLogLevel("ERROR")
    val topic="miu"
    val topics=Array(topic).toSet
    val consumerGroup="mmu"
    val brokerList="192.168.249.10:9092,192.168.249.11:9092"
    val zkHost="192.168.249.10:2181,192.168.249.11:2181"
    // 模型路径
    val modelPath="hdfs://master:9000/movie/model/i_30_l0_0.006_1.197765093217101"
    // 因为使用 kafka direct的方式来连接sparkStreaming
    // 手动管理offset 将offset 存储到zookeeper之中
    val zkClient=getZkClient(zkHost)
    val zkGroupTopicDirs=getZkTopicDirs(consumerGroup,topic)
    // 获得kafka 消费者的目录
    val consumerOffsetDir=zkGroupTopicDirs.consumerOffsetDir
    // 配置kafka的属性
    val kafkaParams=getKafkaParams(zkHost,brokerList,consumerGroup)
    // 获得topic下面的子目录的数量
    val children=getTopicChildrenNum(zkClient,consumerOffsetDir)
    val kafkaStream=getKafkaDirectStream(ssc,kafkaParams,topic,zkClient,children,consumerOffsetDir)
    //  val model=MatrixFactorizationModel.load(ssc.sparkContext,modelPath)
    var td=TrainData.getTrainDf(spark.sparkContext)
    // 获得在训练模型所需要的训练数据的id集合
    val movieIds: Array[Int] = getTopMovieFromMysql(sc)  // 获得被评价次数最多的5个电影，作为不是存在于trainingData之中的数据。
    val mm=TrainModel.getModel(ssc.sparkContext,modelPath)
    //  val broadMovieIds = sc.broadcast(movieIds)  // 将movieIds集合进行广播
    // 这里就是出现了一个问题 使用kafkaProducer 进行数据生产，
    // 我使用这里的代码进行消费的时候 竟然发现 offset 全都被进行更新了
    // 这里的代码是由错误的  创建输入流的时候的问题。后来解决了
    // 计算篇
    kafkaStream.foreachRDD(rdd=>{
      // 获得分区的offset
      var offsetRanges = Array[OffsetRange]()
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      // 数据的去重在7s左右
      val collect: Array[Int] = rdd.map(_._2).map(line=>line.split("_")(0).toInt).collect()
      val userIds = collect.map((_,1)).toMap[Int,Int].keys.toArray[Int] //这样做的话 带宽问题 与 driver的内存问题

      val  movieIds=BroadMovieIds.getBroadMovieIds(rdd.sparkContext).value
      // 登陆的用户id 进行收集
      /*
       val dd= rdd.map(_._2).map(line=>{
        (line.split("_")(0).toInt,"1")
      })
      var map = dd.groupByKey().map(data=>{
        data._1.toInt
      }).collect()
      */
      println("map size is "+userIds.size)
      var tmp=""
      var data=new ArrayBuffer[(Int,String)]()
      var start=System.currentTimeMillis()
      // 获取训练的数据集
      for(i<-userIds){
        val ratings: Array[Rating] = TrainModel.getModel(rdd.sparkContext,modelPath).recommendProducts(i,5)
        tmp=ratings.map(_.product).mkString("_")
        // 判断用户从前没有登陆过的用户
       // val num = TrainData.getTrainDf(rdd.sparkContext).select("user_id").where("user_id="+i).count()
        /*
        if(num==0){  // 没有该用户以前的历史记录
          tmp=movieIds.mkString("_")  // 将评价次数最多的电影推荐给用户
        } else { // 是以前的用户 ，使用模型进行推荐
          val ratings: Array[Rating] = TrainModel.getModel(rdd.sparkContext,modelPath).recommendProducts(i,5)
          tmp=ratings.map(_.product).mkString("_")
        }
        */
        data.append((i,tmp))
      }
      println("推荐消耗时间为:"+(System.currentTimeMillis()-start)/1000.0+" s")
      println("data size "+ data.size)
      // 将数据写入到mysql之中

      //  writeReccommendMovieIdToMysql(rdd.sparkContext,data)
      // 更新offset
      println("start ......")
      val zk=getZkClient(zkHost)
      for(o<-offsetRanges){
        val path=consumerOffsetDir+"/"+o.partition
        // ZkUtils.updatePersistentPath(zk,path,o.fromOffset.toString)
        println(o.fromOffset+"  "+o.untilOffset)
      }
    })
    ssc
  }


  // 这里使用sparkStreaming 作为kafka消费者 进行实时推荐
  def main(args: Array[String]): Unit = {
    val checkPointpath="hdfs://master:9000/movie/tmp"
    val markFile="hdfs://master:9000/movie/stop"
    // 使用checkpoint的方式进行元数据容错
    val ssc=StreamingContext.getOrCreate(checkPointpath,streamingContextFactory _)
    ssc.start()
    stopStreamingContextByMarkFile(ssc,markFile)
    //  ssc.awaitTermination()

  }

}

object BroadMovieIds{
  private var instance:Broadcast[Array[Int]]=null
  def getBroadMovieIds(sc:SparkContext):Broadcast[Array[Int]]={
    if(instance==null){
      synchronized{
        if(instance==null){
          val movieIds: Array[Int] = SparkConsumerV2.getTopMovieFromMysql(sc)
          instance=sc.broadcast[Array[Int]](movieIds)
        }
      }
    }
    instance
  }
}

object  TrainData{
  private  var instance:DataFrame =null
  def getTrainDf(sc:SparkContext):DataFrame={
    if(instance==null){
      synchronized{
        if(instance==null){
          instance = SparkConsumerV2.getTrainUserIds(sc)
          instance=instance.persist(StorageLevel.MEMORY_ONLY) // 有个疑惑就是是否需要将trainDf作为广播变量广播出去？
          //instance.rdd.checkpoint()
          println(instance.count())
        }
      }
    }
    instance
  }
}

object  TrainModel{
  private  var instance:MatrixFactorizationModel=null
  def getModel(sc:SparkContext,modelPath:String)={
    if(instance==null){
      synchronized{
        if(instance==null){
          instance=MatrixFactorizationModel.load(sc,modelPath)
        }
      }
    }
    instance
  }

}
