package com.code.scala

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col

class WriteToHiveInColOrder {

  def getHiveDF(spark: SparkSession, inputDF: DataFrame, schemaName: String, tableName: String): DataFrame = {
    spark.sql(s"use ${schemaName}")
    val resultDF: DataFrame = inputDF.select(spark.table(tableName).columns.map(col): _*)
    resultDF
  }
}
