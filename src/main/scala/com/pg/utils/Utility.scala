package com.pg.utils

import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}

object Utility {

  def readFromSftp(sparkSession: SparkSession, sftpConfig: Config, filename: String): DataFrame = {
    sparkSession.read
      .format("com.springml.spark.sftp")
      .option("host", sftpConfig.getString("hostname"))
      .option("port", sftpConfig.getString("port"))
      .option("username", sftpConfig.getString("username"))
      .option("pem", sftpConfig.getString("pem"))
      .option("fileType", "csv")
      .option("delimiter", "|")
      .load(s"${sftpConfig.getString("directory")}/$filename")
  }

  def writeParquetToS3(dataframe: DataFrame, s3Bucket: String, filename: String) = {
    dataframe.write.option("header", "true").
      partitionBy("mobile_os").
      mode("overwrite").
      parquet(s"s3n://$s3Bucket/$filename")
  }

}
