package dataclean

import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
// 这里使用了MLLib 这个机器学习包 是需要注意的
// 最新的包是ML
object RatingData {
  // 训练模型

  def trainModel(spark:SparkSession,trainNum:Double,testNum:Double):MatrixFactorizationModel={
    val needTrain=spark.sql("select * from sl.trainingdata limit  "+trainNum.toInt)
    val testTrain=spark.sql("select * from sl.testdata limit  " + testNum.toInt)
    // 训练的RDD
    val trainRdd= needTrain.rdd.map{row=>
      Rating(row.getInt(0),row.getInt(1),row.getDouble(2))
    }
    val testCal=needTrain.rdd.map(row=>{
      (row.getInt(0)+"_"+row.getInt(1),row.getDouble(2))
    })
    val testRdd=testTrain.rdd.map{ rdd=>
      (rdd.getInt(0),rdd.getInt(1))
    }
    val lambda=List(0.01, 0.006,0.05,0.03)
    val iteration = List(10, 20,30,40)
    var res=Double.MaxValue
    var lam=0.0
    var ite=0
    var finalModel: MatrixFactorizationModel=null
    trainRdd.cache()
    testRdd.cache()
    testCal.cache()
    for(l<- lambda;i<- iteration){
      val model: MatrixFactorizationModel = ALS.train(trainRdd,1,20,0.001)
      val predict = model.predict(testRdd).map{ x=>
        x match {
          case Rating(user_id, movie_id, rating) => (user_id + "_" + movie_id, rating)
        }
      }
     var tmp=predict.join(testCal).map(x=>{
        val a=x._2._1
        val b=x._2._2
        val c=(a-b)*(a-b)
        c
      }).sum()

      /*
    var tmp= predict.join(testCal).mapPartitions{
       i=>{
         val iterator=i.toIterator
         var sum=0.0
         while(iterator.hasNext){
           val tp=iterator.next()
           val a=tp._2._1
           val b=tp._2._2
           sum+=(a-b)*(a-b)
         }
         List(sum).iterator
       }
    }.sum()
    */
      tmp=tmp/testNum
      tmp=Math.sqrt(tmp)
      if(tmp<res){
        res=tmp
        lam=l
        ite=i
        finalModel=model
      }
    }
    finalModel.save(spark.sparkContext,s"/movie/model/$res")
    finalModel
  }


  def main(args: Array[String]): Unit = {
    //.set("spark.default.parallelism","8")
    val conf=new SparkConf().setAppName("ratingData")
    val spark=SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    var ratings = spark.sql("select user_id,movie_id,rating from sl.ratings")
    val num=ratings.count()
    val percent=0.6  // 设置训练数据 和测试数据的拆分比例
    val trainNum= Math.floor(num * percent)
    val testNum=Math.floor(num * (1-percent))
    // 其实这里就是会遇到一个问题 就是在拆分的时候
    val trainData = ratings.select("*").orderBy(desc("rating"))
    val testData=ratings.select("*").orderBy(asc("rating"))
    import spark.implicits._
    spark.sql("drop table if exists sl.trainingdata")
    spark.sql("drop table if exists sl.testdata")
    trainData.write.mode(SaveMode.Overwrite).format("parquet").saveAsTable("sl.trainingdata")
    testData.write.mode(SaveMode.Overwrite).format("parquet").saveAsTable("sl.testdata")
    trainModel(spark,trainNum,testNum)

  }
}
