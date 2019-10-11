package te

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.SparkSession


/**
  * Created by Administrator on 2016/10/24.
  */
//这是spark新的Als算法的列子
object ALSRecommendNewTest {
  //定义个类，来保存一次评分哈
  case class Rating(userId: Int, movieId: Int, rating: Float, timestamp: Long)
  //把一行转换成一个评分类
  def parseRating(str: String): Rating = {
    val fields = str.split("::")
    assert(fields.size == 4)
    Rating(fields(0).toInt, fields(1).toInt, fields(2).toFloat, fields(3).toLong)
  }
  def main(args:Array[String])={
    //SparkSession是spark2.0的全新切入点，以前都是sparkcontext创建RDD的，StreamingContext，sqlContext，HiveContext。
    //DataDrame提供的API慢慢的成为新的标准API，我们需要1个新的切入点来构建他，这个就是SparkSession哈
    //以前我也没见过
    val spark = SparkSession.builder().config("spark.sql.warehouse.dir","E:/ideaWorkspace/ScalaSparkMl/spark-warehouse").master("local").appName("ALSExample").getOrCreate()
    import spark.implicits._

    //read方法返回的是一个DataFrameReader类，可以转换为DataFrame
    //DataFrameReader类的textFile方法：加载文本数据，返回为Dataset
    //使用一个函数parseRating处理一行数据
    val ratings = spark.read.textFile("data/mllib/sample_movielens_ratings.txt")
      .map(parseRating).toDF()

    val Array(training,test)=ratings.randomSplit(Array(0.8, 0.2))

    // Build the recommendation model using ALS on the training data
    //使用训练数据训练模型
    //这里的ALS是import org.apache.spark.ml.recommendation.ALS，不是mllib中的哈
    //setMaxiter设置最大迭代次数
    //setRegParam设置正则化参数，日lambda这个不是更明显么
    //setUserCol设置用户id列名
    //setItemCol设置物品列名
    //setRatingCol设置打分列名
    val als = new ALS()

    als.setRank(10)
      .setMaxIter(5)
      .setRegParam(0.01)
      .setUserCol("userId")
      .setItemCol("movieId")
      .setRatingCol("rating")

    //fit给输出的数据，训练模型，fit返回的是ALSModel类
    val model = als.fit(training)

    //使用测试数据计算模型的误差平方和
    //transform方法把数据dataset换成dataframe类型，预测数据
    val predictions = model.transform(test)

    //RegressionEvaluator这个类是用户评估预测效果的，预测值与原始值
    //这个setLabelCol要和als设置的setRatingCol一致，不然会报错哈
    //RegressionEvaluator的setPredictionCol必须是prediction因为，ALSModel的默认predictionCol也是prediction
    //如果要修改的话必须把ALSModel和RegressionEvaluator一起修改
    //model.setPredictionCol("prediction")和evaluator.setPredictionCol("prediction")
    //setMetricName这个方法，评估方法的名字，一共有哪些呢？
    //rmse-平均误差平方和开根号
    //mse-平均误差平方和
    //mae-平均距离（绝对）
    //r2-没用过不知道
    //这里建议就是用rmse就好了，其他的基本都没用，当然还是要看应用场景，这里是预测分值就是用rmse。如果是预测距离什么的mae就不从，看场景哈
    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("rating")
      .setPredictionCol("prediction")

    val rmse = evaluator.evaluate(predictions)
    println("Root-mean-square error = "+rmse)

    //stop是停止底层的SparkContext
    spark.stop()
  }
}


