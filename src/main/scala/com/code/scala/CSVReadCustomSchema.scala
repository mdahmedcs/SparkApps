package com.code.scala

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.Column
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.when
import org.apache.spark.sql.functions.min
import org.apache.spark.sql.functions.max
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import java.sql.SQLException;
import org.apache.spark.sql.functions.hash;
import org.apache.spark.sql.functions.hashCode;
import java.text.SimpleDateFormat;
import java.util.Date;


object CSVReadCustomSchema {

  def main(args: Array[String]): Unit = {


    val spark:SparkSession = SparkSession.builder().master("local[4]").appName("demo").getOrCreate()

    val sc = spark.sparkContext
   // sc.setCheckpointDir("E:\\checkpointing")


    //  import spark.sqlContext.implicits._

    sc.setLogLevel("ERROR")

    //Reading from csv by programatically specifying schema

    val schemacsv = StructType(Array(StructField("custnum", IntegerType, nullable=true),StructField("ordernum", IntegerType, nullable=true),StructField("email", StringType, nullable=true),StructField("fname", StringType, nullable=true),
      StructField("lname", StringType, nullable=true),StructField("title", StringType, nullable=true),StructField("company", StringType, nullable=true),StructField("address", StringType, nullable=true),StructField("city", StringType, nullable=true),
      StructField("state", StringType, nullable=true),StructField("zip", StringType, nullable=true),StructField("country", StringType, nullable=true),StructField("phone", StringType, nullable=true),StructField("brand", StringType, nullable=true)))

    val dfsamplecsv=spark.read.format("csv")
      .option("header","true")
      .schema(schemacsv)
      .load("src/main/resources/subscribers2.csv")

    dfsamplecsv.write.mode("overwrite").parquet("src/main/resources/output/")

    //createOrReplaceTempView: The lifetime      bvnof this temporary view is tied to the [[SparkSession]] that was used to create this Dataset.
    //createGlobalTempView: The lifetime of this temporary view is tied to this Spark application.

    println("createorReplaceTempView")
    dfsamplecsv.createOrReplaceTempView("v1")

    val dftempview = spark.sql("select custnum,ordernum,email,state from v1 where lower(state)='tx' limit 25")
    dftempview.persist()
    dftempview.show();





    def currentStamp():String = {
      val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss a")
      return format.format(new Date())

    }

    println(currentStamp())
  }
}