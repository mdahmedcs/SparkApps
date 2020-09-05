package com.code.scala

import org.apache.spark.sql.SparkSession

object Sample {
  def main(args: Array[String]): Unit = {


    val spark: SparkSession = SparkSession.builder().master("local[4]").appName("demo").getOrCreate()

    val sc = spark.sparkContext
  //  sc.setCheckpointDir("E:\\checkpointing\\sample")



    //  import spark.sqlContext.implicits._

    sc.setLogLevel("WARN")

    val df = spark.read.parquet("src/main/resources/output/")

   // df.show(10)
   // df.checkpoint(false)
   // df.cache()
   // df.repartition(10)
    df.write.mode("overwrite").partitionBy("state").parquet("src/main/resources/output2/")

    // df.show(10)

    val df_csv = spark.read.format("parquet").load("src/main/resources/output2/")
   //  df_csv.checkpoint(false)
   // df_csv.filter("state=NJ").show(10)
    df_csv.show(15)
    df_csv.printSchema()

    while(true)
      {


      }
  }

}
