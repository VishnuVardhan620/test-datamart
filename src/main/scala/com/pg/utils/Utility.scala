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

  def readFromRDS(sparkSession: SparkSession, mysqlConfig: Config,dbtable: String, partitionby:String): DataFrame ={

    val host = mysqlConfig.getString("hostname")
    val port = mysqlConfig.getString("port")
    val database = mysqlConfig.getString("database")

    var jdbcParams = Map("url" ->   s"jdbc:mysql://$host:$port/$database?autoReconnect=true&useSSL=false",
      "lowerBound" -> "1",
      "upperBound" -> "100",
      "dbtable" -> s"$database.$dbtable",
      //      "dbtable" -> "(select a, b, c from testdb.TRANSACTIONSYNC where some_cond) as t",
      "numPartitions" -> "2",
     "partitionColumn" -> partitionby,
      "user" -> mysqlConfig.getString("username"),
      "password" -> mysqlConfig.getString("password")
    )

    println("\nReading data from MySQL DB using SparkSession.read.format(),")
   sparkSession
      .read.format("jdbc")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .options(jdbcParams)                                                  // options can pass map
      .load()


  }


  def writeParquetToS3(dataframe: DataFrame, s3Bucket: String, filename: String) = {
    dataframe.write
       // .partitionby("ins_ts")
      //.partitionBy(partitionby)
      .mode("overwrite")
      .parquet(s"s3n://$s3Bucket/PG_DATAMART/$filename")
  }

}
