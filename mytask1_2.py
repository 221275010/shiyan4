from pyspark import SparkContext

# 初始化 SparkContext
sc = SparkContext(appName="ActiveUserAnalysis")

# 读取数据文件
file_path = "file:///home/njucs/myspark/user_balance_table.csv"
data = sc.textFile(file_path)

# 解析 CSV 数据
def parse_line(line):
    fields = line.split(",")
    user_id = fields[0]
    report_date = fields[1]  # 日期格式为 YYYYMMDD
    return (user_id, report_date)

# 解析每一行数据
parsed_data = data.map(parse_line)

# 过滤出 2014年8月的数据，直接用字符串格式 'YYYYMMDD'
august_data = parsed_data.filter(lambda x: "20140801" <= x[1] <= "20140831")

# 这里将所有数据视为"有记录的"数据
user_active_data = august_data.map(lambda x: (x[0], x[1]))  # 以 (user_id, report_date) 为键值对

# 统计每个用户在8月内有多少天有记录
user_active_days = user_active_data.distinct() \
                                   .map(lambda x: (x[0], 1)) \
                                   .reduceByKey(lambda x, y: x + y)

# 过滤出在2014年8月有至少5天记录的用户·
active_users = user_active_days.filter(lambda x: x[1] >= 5)

# 统计活跃用户的总数
active_user_count = active_users.count()

# 输出活跃用户总数
print(f"{active_user_count}")

# 将活跃用户总数保存到 output1_2 目录下
output_path = "file:///home/njucs/myspark/output1_2"
sc.parallelize([f"{active_user_count}"]).repartition(1).saveAsTextFile(output_path)

# 停止 SparkContext
sc.stop()