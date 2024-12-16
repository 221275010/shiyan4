from pyspark import SparkContext

# 初始化 SparkContext
sc = SparkContext(appName="FundsFlowAnalysis")

# 读取数据文件
file_path = "file:///home/njucs/myspark/user_balance_table.csv"
data = sc.textFile(file_path)

# 解析 CSV 数据并提取所需字段
def parse_line(line):
    fields = line.split(",")
    # 取出每一列的数据
    report_date = fields[1]
    total_purchase_amt = int(fields[4])  # 资金流入
    total_redeem_amt = int(fields[8])    # 资金流出
    return (report_date, total_purchase_amt, total_redeem_amt)

# 解析数据并转化为 RDD
parsed_data = data.map(parse_line)

# 聚合数据：按 report_date 聚合并求和资金流入和流出
aggregated_data = parsed_data.map(lambda x: (x[0], (x[1], x[2]))) \
                             .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))

# 按日期升序排序
sorted_data = aggregated_data.sortByKey()

# 输出结果：日期、总资金流入、总资金流出
result = sorted_data.map(lambda x: f"{x[0]} {x[1][0]} {x[1][1]}")


# 打印或保存结果
for line in result.collect():
    print(line)

# 将结果写入到 output1_1 文件
output_path = "file:///home/njucs/myspark/output1_1"
result.coalesce(1).saveAsTextFile(output_path)

# 停止 SparkContext
sc.stop()