package com.code.scala

import org.apache.spark.sql.SparkSession

object WordCount {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder().master("local[4]").appName("demo").getOrCreate()
    val sc = spark.sparkContext
    //sc.setCheckpointDir("")
    //  import spark.sqlContext.implicits._
    sc.setLogLevel("ERROR")
    //word count example
    val text = sc.textFile("E:\\spark_examples\\words.txt")
    text.flatMap(x => x.split(" ")).filter(x => x.equalsIgnoreCase("fathima")).map(y => (y, 1)).reduceByKey(_ + _).sortByKey().collect().foreach(println) //one line computation for word count
  }
}
