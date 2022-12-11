package main.scala.com.code.scala

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import java.text.SimpleDateFormat
import java.time.temporal.ChronoUnit
import java.time.LocalDate

object months_diff {

  def main(args: Array[String]): Unit = {
  val s1="À"
    val s2 = new String(s1.getBytes("UTF-8"), "ISO-8859-1")

    println(s2)

  }

}
