package com.pg

import com.pg.utils.{Constants, Utility}
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.current_date
import scala.collection.JavaConversions._

object SourceDataLoading {
  def main(args: Array[String]): Unit = {
    val rootConfig = ConfigFactory.load("application.conf").getConfig("conf")
    val s3Config = rootConfig.getConfig("s3_conf")
    //val mysqlConfig = rootConfig.getConfig("mysql_conf")
    val sparkSession = SparkSession.builder()
      .master("local")
      .appName("Pampers DataMart")
      .getOrCreate()
    sparkSession.sparkContext.setLogLevel(Constants.ERROR)

    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", s3Config.getString("access_key"))
    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", s3Config.getString("secret_access_key"))

    val s3Bucket = s3Config.getString("s3_bucket")

    val srcList = rootConfig.getStringList("SOURCE_DATA").toList
    for(src <- srcList) {
      val srcConf = rootConfig.getConfig(src)
      src match {
      case "OL" =>
        val olDf = Utility
          .readFromSftp(sparkSession, srcConf.getConfig("sftp_conf"), srcConf.getString("filename"))
          .withColumn("ins_ts", current_date())
        olDf.show()
        Utility.writeParquetToS3(olDf, s3Bucket, "OL")

      case "SB" =>
          val dbtable = "TRANSACTIONSYNC"
          val mysqlDf = Utility.
            readFromRDS(sparkSession, srcConf.getConfig("mysql_conf"), dbtable,"App_Transaction_Id")

          mysqlDf.show()
      Utility.writeParquetToS3(mysqlDf, s3Bucket, "SB")

      }
    }

    sparkSession.stop()
  }

}
