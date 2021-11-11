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

    // Question 1.
    val fst_df = df.select("date", "user_id")
    fst_df.createOrReplaceTempView("date_table")

    val fst_df_count = spark.sql("SELECT date, COUNT(DISTINCT(user_id)) as num_user FROM date_table GROUP BY date ORDER BY num_user DESC")
    fst_df_count.show(1, false)

    fst_df_count.coalesce(1).
      write.
      format("com.databricks.spark.csv").
      option("header", "true").
      save("/user/hjmoon/answer_1.csv")

    val max_num_date = fst_df_count.collect()(0)(0).toString
    println("Date with the most users: " + max_num_date)

    // Question 2.
    val sec_time = udf(UDFs.sec_time _)
    val sess_time = udf(UDFs.sess_time _)
    spark.udf.register("sec_time", sec_time)
    spark.udf.register("sess_time", sess_time)

    val sec_df = df.drop("event_time", "event_type")
      .filter($"date" === max_num_date)
      .withColumn("time", expr("sec_time(hour, min, sec)"))
    sec_df.createOrReplaceTempView("session_table")

    val sec_df_min_max = spark.sql("SELECT user_id, user_session as session_id, MAX(time) as maxTime, min(time) as minTime FROM session_table GROUP BY user_id, user_session")
    sec_df_min_max.createOrReplaceTempView("session_min_max")

    val sec_df_time = spark.sql("SELECT user_id, session_id, sess_time(maxTime, minTime) as session_time FROM session_min_max ORDER BY session_time DESC")
    val sec_df_time_10 = sec_df_time.limit(10)
    sec_df_time_10.show(false)

    sec_df_time_10.coalesce(1).
      write.
      format("com.databricks.spark.csv").
      option("header", "true").
      save("/user/hjmoon/answer_2.csv")

    // Question 3.
    val dev_quarter = udf(UDFs.dev_quarter _)
    val set_quarter_time = udf(UDFs.set_quarter_time _)
    spark.udf.register("dev_quarter", dev_quarter)
    spark.udf.register("set_quarter_time", set_quarter_time)

    val trd_df = df.drop("event_type").drop("user_session")
      .filter($"date" === max_num_date)
      .withColumn("quarter", expr("dev_quarter(min)"))
    trd_df.createOrReplaceTempView("quarter_table")

    var trd_df_quarter = spark.sql("SELECT date, hour, quarter, count(DISTINCT(user_id)) as num_user FROM quarter_table GROUP BY date, hour, quarter ORDER BY hour, quarter")
    trd_df_quarter = trd_df_quarter.withColumn("quarter_time", expr("set_quarter_time(date, hour, quarter)"))
      .withColumn("num_user", $"num_user")
      .drop("date", "hour", "quarter")
    trd_df_quarter.show(100, false)

    trd_df_quarter.coalesce(1).
      write.
      format("com.databricks.spark.csv").
      option("header", "true").
      save("/user/hjmoon/answer_3.csv")

    // Question 4.
    val fth_df = df.filter($"date" === max_num_date)
      .drop("date", "hour", "min", "sec")

    fth_df.show(false)
    fth_df.createOrReplaceTempView("type_table")

    val fth_df_type = spark.sql("SELECT event_type, COUNT(user_id) as num_user FROM type_table GROUP BY event_type ORDER BY num_user DESC")
    fth_df_type.show(false)

    val type_count_list = fth_df_type.collect()

    var view = 0.0
    var cart = 0.0
    var purchase = 0.0
    var view_to_cart = 0.0
    var cart_to_purchase = 0.0

    for(i <- 0 to (type_count_list.length - 1)) {
      val event_type = type_count_list(i)(0).toString
      val event_num = type_count_list(i)(1).toString

      if(event_type == "view")
        view = event_num.toFloat
      else if(event_type == "cart")
        cart = event_num.toFloat
      else if(event_type == "purchase")
        purchase = event_num.toFloat
    }

    view_to_cart = cart / view
    cart_to_purchase = purchase / cart

    val fth_df_funnel = Seq(
      ("view", view, 1),
      ("cart", cart, view_to_cart),
      ("purchase", purchase, cart_to_purchase)
    ).toDF("event_type", "num_user", "funnel")

    fth_df_funnel.coalesce(1).
      write.
      format("com.databricks.spark.csv").
      option("header", "true").
      save("/user/hjmoon/answer_4.csv")

    println("view-to-cart's funnel: " + view_to_cart)
    println("cart-to-purchase's funnel: " + cart_to_purchase)

    spark.stop()
  }
}
