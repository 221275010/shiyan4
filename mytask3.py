from pyspark import SparkContext
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, DoubleType

# 初始化 SparkContext 和 SparkSession
sc = SparkContext(appName="FundsFlowPrediction")
spark = SparkSession(sc)

# 读取数据文件
file_path = "file:///home/njucs/myspark/user_balance_table.csv"
data = sc.textFile(file_path)

# 解析 CSV 数据并提取所需字段
def parse_line(line):
    fields = line.split(",")
    report_date = int(fields[1])  # 日期已经是 int 类型
    total_purchase_amt = int(fields[4])  # 资金流入
    total_redeem_amt = int(fields[8])    # 资金流出
    return (report_date, total_purchase_amt, total_redeem_amt)

# 解析数据并转化为 RDD
parsed_data = data.map(parse_line)

# 聚合数据：按 report_date 聚合并求和资金流入和流出
aggregated_data = parsed_data.map(lambda x: (x[0], (x[1], x[2]))) \
                             .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))

# 转换为 DataFrame
df = aggregated_data.map(lambda x: (x[0], x[1][0], x[1][1])).toDF(["date", "total_purchase_amt", "total_redeem_amt"])

# 特征工程：创建特征向量
assembler = VectorAssembler(inputCols=["date"], outputCol="features")

# 创建线性回归模型
lr_purchase = LinearRegression(featuresCol="features", labelCol="total_purchase_amt")
lr_redeem = LinearRegression(featuresCol="features", labelCol="total_redeem_amt")

# 创建 Pipeline
pipeline_purchase = Pipeline(stages=[assembler, lr_purchase])
pipeline_redeem = Pipeline(stages=[assembler, lr_redeem])

# 分割数据集
train_data, test_data = df.randomSplit([0.7, 0.3])

# 训练模型
model_purchase = pipeline_purchase.fit(train_data)
model_redeem = pipeline_redeem.fit(train_data)

# 预测20140901到20140930的数据
date_range = [20140901 + i for i in range(30)]
prediction_data = spark.createDataFrame([(date,) for date in date_range], ["date"])

predictions_purchase = model_purchase.transform(prediction_data)
predictions_redeem = model_redeem.transform(prediction_data)

# 重命名列
predictions_purchase = predictions_purchase.withColumnRenamed("prediction", "total_purchase_amt")
predictions_redeem = predictions_redeem.withColumnRenamed("prediction", "total_redeem_amt")

# 选择需要的列
predictions_purchase = predictions_purchase.select("date", "total_purchase_amt")
predictions_redeem = predictions_redeem.select("date", "total_redeem_amt")

# 输出预测结果到控制台
predictions_purchase.show(n=100)
predictions_redeem.show(n=100)

# 将结果写入 CSV 文件
output_path_purchase = "file:///home/njucs/myspark/output_predictions_purchase.csv"
output_path_redeem = "file:///home/njucs/myspark/output_predictions_redeem.csv"

predictions_purchase.coalesce(1).write.csv(output_path_purchase, header=True, mode="overwrite")
predictions_redeem.coalesce(1).write.csv(output_path_redeem, header=True, mode="overwrite")

# 停止 SparkContext
sc.stop()