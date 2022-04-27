package com.code.scala

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object Sample {
  def main(args: Array[String]): Unit = {


    val spark: SparkSession = SparkSession.builder().master("local[4]").appName("demo").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    val schema = StructType(Array(StructField("custnum", IntegerType, nullable=true),StructField("ordernum", IntegerType, nullable=true),StructField("email", StringType, nullable=true),StructField("fname", StringType, nullable=true),
      StructField("lname", StringType, nullable=true),StructField("title", StringType, nullable=true),StructField("company", StringType, nullable=true),StructField("address", StringType, nullable=true),StructField("city", StringType, nullable=true),
      StructField("state", StringType, nullable=true),StructField("zip", StringType, nullable=true),StructField("country", StringType, nullable=true),StructField("phone", StringType, nullable=true),StructField("brand", StringType, nullable=true)))

    val df=spark.read.format("csv")
      .option("header","true")
      .schema(schema)
      .load("src/main/resources/subscribers2.csv")


    print(df.count())

    df.printSchema()


  }

}
