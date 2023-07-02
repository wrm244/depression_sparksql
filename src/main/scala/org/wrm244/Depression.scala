package org.wrm244

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.expr

object Depression extends Serializable {
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    //编码阶段创建的SparkSession对象
    val sparkSession: SparkSession =
      SparkSession.builder().master("local[*]")
        .appName("GlobalTemperatureProcess")
        .getOrCreate()

    import sparkSession.implicits._
    import org.apache.spark.sql.functions._
    //加载数据源
    val inputWebfile = "data/depression.csv"
    var AgeData: DataFrame = sparkSession.read.format("csv")
      .option("header", true)
      .option("multiLine", true)
      .load(inputWebfile)

    //正则表达式的替换

    AgeData = AgeData.withColumn("age", regexp_replace($"age", " years", ""))
    AgeData.show(5)

    //3.注册临时表
    AgeData.createOrReplaceTempView("tbl_DepressionTemperatures")
    //统计age与抑郁症患者数量的关系
    var age_depressed_result: DataFrame =
      sparkSession.sql(
        """
          |select age,CAST(SUM(depressed_no) AS INT) as total_depressed
          |from tbl_DepressionTemperatures
          |GROUP BY age;
          | """.stripMargin)

    age_depressed_result.show(false)
    //写入数据库
    //DBTools.WriteMySql("age_depressed", age_depressed_result)
    //统计HDI与抑郁症患者数量的关系
    val hdi_depressed_result: DataFrame =
    sparkSession.sql(
      """
        |SELECT CONCAT_WS('-', FLOOR((`HDI for year` - 0.4) / 0.05) * 0.05 + 0.4, FLOOR((`HDI for year` - 0.4) / 0.05) * 0.05 + 0.45) AS HDI, CAST(SUM(`depressed_no`) AS INT) AS total_depressed
        |FROM tbl_DepressionTemperatures
        |WHERE `HDI for year` IS NOT NULL AND `HDI for year` != '' AND `depressed_no` IS NOT NULL AND `depressed_no` != ''
        |GROUP BY FLOOR((`HDI for year` - 0.4) / 0.05)
        |ORDER BY HDI ASC;
        | """.stripMargin)
    hdi_depressed_result.show(false)
    //写入数据库
    //DBTools.WriteMySql("HDI_depressed", hdi_depressed_result)
  }
}
