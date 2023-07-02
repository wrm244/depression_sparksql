package org.wrm244

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object DBTools {
  /**
   * 功能：获取Session对象
   * @param appname
   * @param master
   */
  def getSession(appname:String,master:String) :SparkSession= {
    val session: SparkSession =
      SparkSession.builder().master(master).appName(appname).getOrCreate()
    session
  }

  /**
   * 分析结果写入MySQL数据库
   * @param tableName
   * @param result
   */
  def WriteMySql(tableName:String,result:DataFrame): Unit ={
    result.write
      .format("jdbc")
      .option("url","jdbc:mysql://guet.gxist.cn:3306/depression_db")
      .option("driver","com.mysql.cj.jdbc.Driver")
      .option("user","root")
      .option("password","562mmNRTntGrzF8C")
      .option("dbtable",tableName)
      .mode(SaveMode.Overwrite)
      .save()
  }
}
