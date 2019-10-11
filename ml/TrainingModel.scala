package ml

import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.sql.SparkSession
//训练模型
object TrainingModel {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("trainingModel")
    val spark=SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    spark.sparkContext.setCheckpointDir("hdfs://master:9000/tmp/check")
    var ratings = spark.sql("select user_id,movie_id,rating from sl.ratings")
    val num=ratings.count()
    val percent=0.6    // 这个是拆分的比例
    val trainNum= Math.floor(num * percent)
    val testNum=Math.floor(num * (1-percent))
    // 这里切分数据集的时候，会遇到问题
    // 因为会有两种解决方法
    // 1：就是直接从原始的数据集之中 进行查询排序
    // 2： 就是首先对原始数据进行不同的排序 保存到两个中间表
    // 然后获取训练数据集和测试数据集就是按照相同的数目来进行查询。
    // 第一种方法会导致内存溢出，原因：limit后面接的数目太大了。
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
    var res=Double.MaxValue
    // 设置als模型的参数 随着迭代次数的增多 DAG会逐步加深 导致栈溢出，所以需要设置checkPoint目录
    val als=new ALS()
    als.setCheckpointInterval(5).setRank(10).setIterations(30).setLambda(0.006)
    val model: MatrixFactorizationModel= als.run(trainRdd)
    val predict = model.predict(testRdd).map { x =>
      x match {
        case Rating(user_id, movie_id, rating) => (user_id + "_" + movie_id, rating)
      }
    }
    // 对训练的模型 使用RMSE 进行凭借
    var tmp = predict.join(testCal).map(x => {
      val a = x._2._1
      val b = x._2._2
      val c = (a - b) * (a - b)
      c
    }).mean()
    res=Math.sqrt(tmp)
    // 模型保存
    model.save(spark.sparkContext,s"/movie/model/i_30_l0_0.006_$res")
  }
}
