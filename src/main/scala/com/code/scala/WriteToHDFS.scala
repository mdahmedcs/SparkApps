package com.code.scala

import org.apache.log4j.Logger
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SparkSession}

class WriteToHDFS {
  val logger = Logger.getLogger(this.getClass)


  /** This function writes file on hdfs with name and format provided in input parameter
   *
   * @param spark    : SparkSession
   * @param inputDF  : input dataframe
   * @param hdfsPath : hdfs location for saving the file
   * @param fileName : extract file name
   */
  def writeToHdfs(spark: SparkSession, inputDF: DataFrame, hdfsPath: String, fileFormat: String, fileName: String): Unit = {
    logger.info("writeToHdfs method started")
    logger.info(s"HDFS Directory Path: ${hdfsPath}")
    inputDF.repartition(1)
      .write
      .format(fileFormat)
      .mode("overwrite")
      .save(s"$hdfsPath/temp")
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val filepath = fs.globStatus(new Path(s"${hdfsPath}/temp/part-*"))(0).getPath
    val targetFilePath = s"${hdfsPath}/${fileName}"
    logger.info(s"spark HDFS file path: ${filepath}")
    logger.info(s"target HDFS file path: ${targetFilePath}")
    logger.info("deleting old target file if already exists")
    fs.delete(new Path(s"${targetFilePath}"), true)
    fs.rename(filepath, new Path(s"${targetFilePath}"))
    logger.info("removing HDFS temp path")
    fs.delete(new Path(s"$hdfsPath/temp"), true)
    logger.info(s"target file created: ${targetFilePath}")
    logger.info("writeToHdfs method completed")
  }
}
