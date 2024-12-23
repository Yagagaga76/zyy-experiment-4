from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import IntegerType, DateType, StructType, StructField, DoubleType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
from datetime import datetime, timedelta

def main():
    conf = SparkConf().setAppName("Finance Purchase and Redeem Prediction")
    sc = SparkContext(conf=conf)
    spark = SparkSession(sc)

    data_path = "hdfs://localhost:9000/user/hadoop/user_balance_table.csv"
    lines = sc.textFile(data_path)
    def parse_line(line):
        fields = line.split(',')
        try:
            report_date = int(fields[1])
            total_purchase_amt = float(fields[8])
            total_redeem_amt = float(fields[13])
            return (report_date, total_purchase_amt, total_redeem_amt)
        except:
            return None

    transactions = lines.map(parse_line).filter(lambda x: x is not None)

    schema = StructType([
        StructField("report_date", IntegerType(), True),
        StructField("total_purchase_amt", DoubleType(), True),
        StructField("total_redeem_amt", DoubleType(), True)
    ])
    df = spark.createDataFrame(transactions, schema)

    vectorAssembler = VectorAssembler(inputCols=["report_date"], outputCol="features")
    df_vector = vectorAssembler.transform(df)

    train_data, test_data = df_vector.randomSplit([0.8, 0.2], seed=42)

    lr_purchase = LinearRegression(featuresCol='features', labelCol='total_purchase_amt')
    lr_redeem = LinearRegression(featuresCol='features', labelCol='total_redeem_amt')

    model_purchase = lr_purchase.fit(train_data)
    model_redeem = lr_redeem.fit(train_data)

    date_range = [datetime(2014, 9, 1) + timedelta(days=x) for x in range(30)]
    predict_df = spark.createDataFrame([(int(d.strftime('%Y%m%d')),) for d in date_range], ["report_date"])
    predict_features = vectorAssembler.transform(predict_df)

    predictions_purchase = model_purchase.transform(predict_features)
    predictions_redeem = model_redeem.transform(predict_features)

    predictions = predictions_purchase.select("report_date", F.col("prediction").alias("purchase")).join(
        predictions_redeem.select("report_date", F.col("prediction").alias("redeem")), "report_date"
    )
    predictions.write.csv("hdfs://localhost:9000/user/hadoop/tc_comp_predict_table.csv", header=True)


    sc.stop()

if __name__ == "__main__":
    main()
