package dataclean

import casemodel.{Links, Movies, Ratings, Tags}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object ETL {

  def readFile(spark:SparkSession,fileName:String,minPartitions:Int):RDD[String]={
     spark.sparkContext.textFile(fileName,minPartitions)
  }

  def saveTable(data:DataFrame,fileMode:String,fileType:String,tableName:String)={
    data.write.mode(fileMode).format(fileType).saveAsTable(tableName)
  }



  def main(args: Array[String]): Unit = {
    val local="local[*]"
    val cluster="yarn-cluster"
     val movieFilePath="hdfs://master:9000/movie/data/movies.txt"
     val ratingsFilePath="hdfs://master:9000/movie/data/ratings.txt"
     val tagsFilePath="hdfs://master:9000/movie/data/tags.txt"
     val conf=new SparkConf().setAppName("ETL")
    val filePath="hdfs://master:9000/movie/data/links.txt"
    val spark=SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    import spark.implicits._

    val minPartitions:Int=8
     // 读取文本文件 并且转换为dataFrame 使用case class的方式还是不错的
    val links=readFile(spark,filePath,minPartitions)
      .map(x=>x.split(",")).filter(x=>{
      x.length > 2
    }).map(x=>Links(x(0).trim.toInt,x(1).toString.trim,x(2).trim)).toDF()

    spark.sql("drop table if exists sl.links")
    // 以表格形式存储的时候 几个分区 最后存储的文件的数目就是几个
    // format 指定存储的格式 很不错
    val fileMode="overwrite"
    val fileType="parquet"
    saveTable(links,fileMode,fileType,"sl.links")
    // 建立电影表

    def rebu(line:String):String={
      var retn:String=""
      val fields=line.replace("\"","").split(",")
      retn=fields(0)+","
      for(i<-1 to fields.length-2){
        retn=retn+fields(i)+":"
      }
      retn=retn.substring(0,retn.length-1)
      retn=retn+","+fields(fields.length-1)
      retn
    }

    val movies=readFile(spark,movieFilePath,minPartitions)
      .map(x=>rebu(x))
      .map(x=>x.split(",")).filter(x=>{
        x.length > 2
    }).map(x=>Movies(x(0).trim.toInt,x(1).trim,x(2).trim)).toDF()
    spark.sql("drop table if exists sl.movies")
    saveTable(movies,fileMode,fileType,"sl.movies")


   def rebuild(str:String):String={
      val fields=str.split(",")
      if(fields.length<=4){
        str
      }
      val sb=new StringBuffer()
      sb.append(fields(0)+",")
      sb.append(fields(1)+",")
      for(i<- 2 to fields.length-2){
        fields(i)=fields(i).replace("\"","").trim()
        sb.append(fields(i)+"|")
      }
      val tmp=sb.substring(0,sb.length()-1)
      val retn =tmp+","+fields(fields.length-1)
      retn
    }



    // 建立tags表格
    val tags=readFile(spark,tagsFilePath,minPartitions)
      .map(x=>rebuild(x))
      .map(x=>x.split(",")).filter(x=>{
        x.length > 3
    }).map(x=>Tags(x(0).trim().toInt,x(1).trim.toInt,x(2).trim,x(3).trim)).toDF()
     spark.sql("drop table if exists sl.tags")
    saveTable(tags,fileMode,fileType,"sl.tags")

    // 建立ratings 表格
    val ratings=readFile(spark,ratingsFilePath,minPartitions)
      .map(x=>x.split(","))
      .filter(x=>{
        x.length >3
      }).map(x=>Ratings(x(0).trim.toInt,x(1).trim.toInt,x(2).trim.toDouble,x(3).trim)).toDF()
    spark.sql("drop table if exists sl.ratings")
    saveTable(ratings,fileMode,fileType,"sl.ratings")

  }

}
