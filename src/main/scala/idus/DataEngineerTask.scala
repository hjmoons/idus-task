package idus

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DataEngineerTask {
  def main(args: Array[String]): Unit = {
    // Spark session start
    val spark = SparkSession
      .builder()
      .appName("ECommerce Data")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    // Load data in HDFS
    val df = spark
      .read
      .option("header", "true")
      .csv("/user/hjmoon/2019-Nov.csv")

    val re_time = udf(DataPreprocess.re_time _)
    spark.udf.register("re_time", re_time)

    // 데이터 전처리 [event_time, event_type, user_id, user_session, month, day, hour, min, sec]
    val df_1 = df.drop("product_id").drop("category_id").drop("category_code").drop("brand").drop("price")
      .withColumn("event_time", expr("re_time(event_time)"))
      .withColumn("event_time", from_utc_timestamp(col("event_time"), "Asia/Seoul"))
      .withColumn("month", date_format(col("event_time"), "MM"))
      .withColumn("day", date_format(col("event_time"), "dd"))
      .withColumn("hour", date_format(col("event_time"), "HH"))
      .withColumn("min", date_format(col("event_time"), "mm"))
      .withColumn("sec", date_format(col("event_time"), "ss"))


    df_1.show(10, false)

    spark.stop()
  }
}
