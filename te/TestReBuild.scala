package te

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TestReBuild {
  def main(args: Array[String]): Unit = {
     val conf=new SparkConf().setAppName("local").setMaster("local[*]")
     val sc=new SparkContext(conf)
     val rdd: RDD[Int] = sc.parallelize( 1 to 10,2)
     val bdd=sc.parallelize(5 to 10,2) //黑名单

     val prdd=rdd.map(x=>(x,1))
     val pbdd=bdd.map(x=>(x,1))

    val joinrdd: RDD[(Int, (Option[Int], Int))] = pbdd.rightOuterJoin(prdd)

    val c= rdd.filter(x=>{
         if(x > 5){
           true
         }else {
           false
         }
     })
    print(c.count())
  // join 的时候 别忘记了 if else 进行组队 否则的话，就是会很惨。
    val bcc= joinrdd.filter(data=>{

       if(data._2._1.isEmpty){
         println("say good")
         true
       }else{
       println(data._2._1.isEmpty)
       false
       }
    })
  print(bcc.count())


    /*
    join.map(data=>{
      (data._1,data._2._2)
    }).foreach(x=>println(x._1))
*/
    sc.setLogLevel("ERROR")

        /*
     rdd.filter(x=>{
        if(x>5){
          return true
        }
        return false
     })
     */

  }
}
