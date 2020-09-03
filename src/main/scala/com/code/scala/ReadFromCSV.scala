package com.code.scala

import org.apache.spark.sql.SparkSession
import java.text.SimpleDateFormat

object ReadFromCSV {

  def current_month(): Int = {

    val date_format = new SimpleDateFormat("YMM")

    return Integer.parseInt(date_format.format(java.util.Calendar.getInstance().getTime()))
  }

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder().master("local[4]").appName("demo").getOrCreate()

    val sc = spark.sparkContext
    sc.setCheckpointDir("E:\\checkpointing")

    //import spark.sqlContext.implicits._

    sc.setLogLevel("WARN")

    val query = """(select latest.* from (select * , dense_rank() over (partition by cyclecode order by cycledate desc) as r1 from cb) as latest where cycledate=DATE_FORMAT(current_date,"%Y%m")-1 and r1=1) cb"""

   // val today = java.util.Calendar.getInstance().getTime()

    val cb0_date = current_month-6
    val cb1_date = current_month-1
    val cb2_date = current_month-2

    println(cb0_date)
    println(cb1_date)
    println(cb2_date)

    val cb0 = """(select * from cb where cycledate>=""" + cb0_date + """) cb0"""
    val cb1 = """(select * from cb where cycledate=""" + cb1_date + """) cb0"""
    val cb2 = """(select * from cb where cycledate=""" + cb2_date + """) cb0"""

    val df = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/test")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", cb0)
      .option("user", "ahmed")
      .option("password", "123456")
      .load()

    df.show()
    df.cache()
    df.createOrReplaceTempView("cb0")

    spark.sql("select latest.* from (select * , dense_rank() over (partition by cycledate order by cyclecode desc) as r1 from cb0) as latest where  r1=1 ").show()


  }
}
