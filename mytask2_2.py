from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, row_number
from pyspark.sql.window import Window

# 初始化SparkSession
spark = SparkSession.builder.appName("CityTopUsers").getOrCreate()

# 1. 加载用户信息表（user_profile_table.csv）
user_profile_columns = ["user_id", "Sex", "City", "constellation"]
user_profile_df = spark.read.option("header", "false").csv("file:///home/njucs/myspark/user_profile_table.csv")
user_profile_df = user_profile_df.toDF(*user_profile_columns)

# 2. 加载用户余额表（user_balance_table.csv）
user_balance_columns = [
    "user_id", "report_date", "tBalance", "yBalance", "total_purchase_amt", "direct_purchase_amt",
    "purchase_bal_amt", "purchase_bank_amt", "total_redeem_amt", "consume_amt", "transfer_amt", 
    "tftobal_amt", "tftocard_amt", "share_amt", "category1", "category2", "category3", "category4"
]
user_balance_df = spark.read.option("header", "false").csv("file:///home/njucs/myspark/user_balance_table.csv")
user_balance_df = user_balance_df.toDF(*user_balance_columns)

# 3. 筛选2014年8月的数据并计算总流量
user_balance_df_aug = user_balance_df.filter(col("report_date").like("201408%")) \
                                      .withColumn("total_flow", col("total_purchase_amt") + col("total_redeem_amt"))

# 4. 将用户信息与余额信息进行连接，获取城市信息
joined_df = user_balance_df_aug.join(user_profile_df, on="user_id", how="inner")

# 5. 按用户ID计算每个用户在8月的总流量
user_total_flow_df = joined_df.groupBy("user_id") \
                               .agg(sum("total_flow").alias("total_flow"))

# 6. 按城市和总流量降序排列，并使用窗口函数对每个城市内的用户进行排名
window_spec = Window.partitionBy("City").orderBy(col("total_flow").desc())

ranked_df = user_total_flow_df.withColumn("rank", row_number().over(window_spec))

# 7. 过滤出每个城市排名前3的用户
top_3_users_df = ranked_df.filter(col("rank") <= 3)

# 8. 输出每个城市总流量排名前三的用户ID及其总流量
result = top_3_users_df.select("City", "user_id", "total_flow")

# 输出结果（可以打印前100行）
result.show(n=100, truncate=False)

# 9. 将结果保存到文件
result.coalesce(1).write.mode("overwrite").csv("file:///home/njucs/myspark/output2_2", header=True)

# 停止Spark会话
spark.stop()