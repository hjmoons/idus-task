package idus

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._


object DataEngineerTask {
  def main(args: Array[String]): Unit = {
    // Spark session start
    val spark = SparkSession
      .builder()
      .appName("ECommerce Data")
      .getOrCreate()

    import spark.implicits._

    // Load data in HDFS
    val load_data = spark
      .read
      .option("header", "true")
      .csv("/user/hjmoon/2019-Nov.csv")
      .toDF

    val re_time = udf(UDFs.re_time _)
    spark.udf.register("re_time", re_time)

    // 데이터 전처리 [event_time, event_type, user_id, user_session, day, hour, min, sec]
    val df = load_data.drop("product_id", "category_id", "category_code", "brand", "price")
      .withColumn("event_time", expr("re_time(event_time)"))
      .withColumn("event_time", from_utc_timestamp($"event_time", "Asia/Seoul"))
      .withColumn("date", date_format($"event_time", "yyyy-MM-dd"))
      .withColumn("hour", date_format($"event_time", "HH").cast("Integer"))
      .withColumn("min", date_format($"event_time", "mm").cast("Integer"))
      .withColumn("sec", date_format($"event_time", "ss").cast("Integer"))

    df.show(false)

    /*
    // Question 1.
    val fst_df = df.select("date", "user_id").groupBy("date")
    val fst_df_count = fst_df.agg(countDistinct("user_id").as("num_user")).orderBy(desc("num_user"))
    fst_df_count.show(1, false)

    // Question 2.
    val sec_time = udf(UDFs.sec_time _)
    val sess_time = udf(UDFs.sess_time _)
    spark.udf.register("sec_time", sec_time)
    spark.udf.register("sess_time", sess_time)

    val sec_df = df.drop("event_time", "event_type")
      .filter($"date" === "2019-11-17")
      .withColumn("time", expr("sec_time(hour, min, sec)"))

    /*
    val sec_df_min_max = sec_df.groupBy("user_id", "user_session")
      .agg(max("time").as("maxTime"), min("time").as("minTime"))

    val sec_df_join = sec_df_min_max.join(sec_df, "user_session")

    sec_df.show(false)
    sec_df_min_max.show(false)
    sec_df_join.show(false)

    val sec_df_time = sec_df_join.withColumn("session_time", expr("sess_time(maxTime, minTime)"))
      .orderBy(desc("session_time"))
      .withColumn("session_id", $"user_session")
      .select("user_id", "session_id", "session_time")

    sec_df_time.show(10, false)

    */

    sec_df.show(false)
    sec_df.createOrReplaceTempView("session_table")

    val sec_df_min_max = spark.sql("SELECT user_id, user_session as session_id, max(time) as maxTime, min(time) as minTime FROM session_table GROUP BY user_id, user_session")

    sec_df_min_max.show()
    sec_df_min_max.createOrReplaceTempView("session_min_max")

    val sec_df_time = spark.sql("SELECT user_id, session_id, sess_time(maxTime, minTime) as session_time FROM session_min_max ORDER BY session_time DESC")
    sec_df_time.show(10, false)

    */

    // Question 3.
    val dev_quarter = udf(UDFs.dev_quarter _)
    val set_quarter_time = udf(UDFs.set_quarter_time _)
    spark.udf.register("dev_quarter", dev_quarter)
    spark.udf.register("set_quarter_time", set_quarter_time)

    val trd_df = df.drop("event_type").drop("user_session")
      .filter($"date" === "2019-11-17")
      .withColumn("quarter", expr("dev_quarter(min)"))

    trd_df.show(false)
    trd_df.createOrReplaceTempView("quarter_table")

    var trd_df_quarter = spark.sql("SELECT date, hour, quarter, count(DISTINCT(user_id)) as num_user FROM quarter_table GROUP BY date, hour, quarter ORDER BY hour, quarter")
    trd_df_quarter = trd_df_quarter.withColumn("quarter_time", expr("set_quarter_time(date, hour, quarter)"))
      .withColumn("num_user", $"num_user")
      .drop("date", "hour", "quarter")
    trd_df_quarter.show(100, false)

    spark.stop()
  }
}
