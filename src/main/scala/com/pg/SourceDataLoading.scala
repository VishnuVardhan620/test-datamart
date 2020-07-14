package com.pg

import com.pg.utils.{Constants, Utility}
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.current_date

object SourceDataLoading {
  def main(args: Array[String]): Unit = {
    val rootConfig = ConfigFactory.load("application.conf").getConfig("conf")
    val s3Config = rootConfig.getConfig("s3_conf")

    val sparkSession = SparkSession.builder()
      .master("local")
      .appName("Pampers DataMart")
      .getOrCreate()
    sparkSession.sparkContext.setLogLevel(Constants.ERROR)

    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", s3Config.getString("access_key"))
    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", s3Config.getString("secret_access_key"))

    val olDf = Utility
      .readFromSftp(sparkSession, rootConfig.getConfig("sftp_conf"), "receipts_delta_GBR_14_10_2017.csv")
        .withColumn("ins_ts", current_date())
    olDf.show()

  }

}
