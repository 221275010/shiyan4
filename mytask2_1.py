from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg

# 初始化 Spark 会话
spark = SparkSession.builder \
    .appName("AverageBalanceByCity") \
    .getOrCreate()

# 1. 手动指定列名并加载用户信息表（user_profile_table.csv）
user_profile_columns = ["user_id", "Sex", "City", "constellation"]
user_profile_df = spark.read.option("header", "false").csv("file:///home/njucs/myspark/user_profile_table.csv")
user_profile_df = user_profile_df.toDF(*user_profile_columns)

# 2. 手动指定列名并加载用户余额表（user_balance_table.csv）
user_balance_columns = [
    "user_id", "report_date", "tBalance", "yBalance", "total_purchase_amt", "direct_purchase_amt",
    "purchase_bal_amt", "purchase_bank_amt", "total_redeem_amt", "consume_amt", "transfer_amt", 
    "tftobal_amt", "tftocard_amt", "share_amt", "category1", "category2", "category3", "category4"
]
user_balance_df = spark.read.option("header", "false").csv("file:///home/njucs/myspark/user_balance_table.csv")
user_balance_df = user_balance_df.toDF(*user_balance_columns)

# 3. 转换列数据类型
user_profile_df = user_profile_df.withColumn("user_id", col("user_id").cast("string")) \
    .withColumn("City", col("City").cast("string"))

user_balance_df = user_balance_df.withColumn("user_id", col("user_id").cast("int")) \
    .withColumn("report_date", col("report_date").cast("string")) \
    .withColumn("tBalance", col("tBalance").cast("int"))

# 4. 过滤出2014年3月1日的数据（注意：report_date 格式为 20140301）
user_balance_filtered_df = user_balance_df.filter(col("report_date") == "20140301")

# 5. 使用 user_id 连接 user_profile_df 和 user_balance_filtered_df
joined_df = user_profile_df.join(user_balance_filtered_df, on="user_id", how="inner")

# 6. 按城市计算每个城市的平均余额
city_avg_balance_df = joined_df.groupBy("City").agg(avg("tBalance").alias("avg_balance"))

# 7. 按平均余额降序排列
city_avg_balance_df_sorted = city_avg_balance_df.orderBy(col("avg_balance").desc())

# 8. 输出结果
city_avg_balance_df_sorted.show(truncate=False)

# 9. 将结果保存到文件

city_avg_balance_df_sorted.coalesce(1).write.mode("overwrite").csv("file:///home/njucs/myspark/output2_1", header=True)

# 停止 Spark 会话
spark.stop()