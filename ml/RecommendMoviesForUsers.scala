package ml





import casemodel.Result
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import java.util.Properties

import dao.RecUserImpl
import daomain.RecUser

// 离线进行推荐
object RecommendMoviesForUsers {
  def writToMysql(spark:SparkSession,data:ArrayBuffer[(Int,String)])={
    import spark.implicits._
    val value: RDD[(Int, String)] = spark.sparkContext.parallelize(data.toSeq,1)
    val movieDf= value.map(data=>{
       Result(data._1,data._2)
    }).toDF()
    val url="jdbc:mysql://192.168.249.10:3306/movie"
    val tableName="rec_user"
    val properties=new Properties()
    properties.setProperty("user","root")
    properties.setProperty("password","111111")
    properties.setProperty("driver", "com.mysql.jdbc.Driver")
    movieDf.write.mode(SaveMode.Append).jdbc(url,tableName,properties)
  }

  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("recommendMoviesForUser")
    val spark=SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    spark.sparkContext.setCheckpointDir("hdfs://master:9000/tmp/check")
    val recommendNum=5
    val modelPath="/movie/model/i_30_l0_0.006_1.197765093217101"
    var trainDf=spark.sql("select user_id from sl.trainingdata group by user_id")
    trainDf=trainDf.cache()
    // 模型的加载
    val model=MatrixFactorizationModel.load(spark.sparkContext,modelPath)
    val tp=trainDf.rdd.map(row=>row.getInt(0))
    tp.checkpoint()
    // 在driver 端进行推荐 如果在froeach调用 model的推荐的话，
    // 就是会报错。
    val userIterator = tp.toLocalIterator
    val container=new ArrayBuffer[(Int,String)]()
    while(userIterator.hasNext){
       val userId=userIterator.next()
       val ratings: Array[Rating] = model.recommendProducts(userId,recommendNum)
       val sb=new mutable.StringBuilder()
       for(i <-0 to ratings.length-1){
          sb.append("movie_id="+ratings(i).product+","+"rating="+ratings(i).rating+"|")
       }
      container.append((ratings(0).user,sb.substring(0,sb.length-1)))

      // 一次写入500条
      if(container.length>=1000){
        writToMysql(spark,container)
        container.clear()
      }
    }
    // 将容器之中剩余的数据写入mysql之中。
    if(container.length>0){
      writToMysql(spark,container)
    }

  }
}
