package te

import org.apache.spark.SparkConf
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.SparkSession

// spark ml类库之中的模型训练
object TrainModel2 {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("trainingModel")
    val spark=SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    spark.sparkContext.setCheckpointDir("hdfs://master:9000/tmp/check")
    var ratings = spark.sql("select user_id,movie_id,rating from sl.ratings")
    // 将训练数据划分
    val Array(train,test) = ratings.randomSplit(Array(0.6,0.4))
    // 设置als模型的参数 随着迭代次数的增多 DAG会逐步加深 导致栈溢出，所以需要设置checkPoint目录
    val als=new ALS()
    als.setCheckpointInterval(5)
      .setRank(10)
      .setMaxIter(30)
      .setRegParam(0.06)
      .setUserCol("user_id")
      .setItemCol("movie_id")
      .setRatingCol("rating")
    // 对训练数据进行训练
    val model = als.fit(train)
    //接下来是模型评估
    // 1:对测试集数据进行预测 将预测的结果 和测试集的原本的结果进行比较 使用rmse作为评估模型好坏的根据
    val prediction = model.transform(test)
    val executor=  new RegressionEvaluator()
      .setPredictionCol("prediction")
      .setLabelCol("rating")
      .setMetricName("rmse")
    val grade: Double = executor.evaluate(prediction)
    spark.stop()
  }
}
