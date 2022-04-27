package main.scala.com.code.scala

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions.{col, lit, map}

object check_calendar {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder().master("local[4]").appName("demo").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("OFF")

    val schema = StructType(Array(StructField("cycle_code", IntegerType, nullable = true), StructField("process_date", IntegerType, nullable = true)))

    val df = spark.read.format("csv")
      .option("header", "true")
      .schema(schema)
      .load("src/main/resources/calendar.csv")

    val result = df.filter(col("process_date") === 20220430).isEmpty

    print(result)
  }

}
