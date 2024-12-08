from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder.appName("Average Balance by City").getOrCreate()

    # 加载数据
    user_profile_df = spark.read.csv("hdfs://localhost:9000/user/hadoop/user_profile_table.csv", header=True, inferSchema=True)
    user_balance_df = spark.read.csv("hdfs://localhost:9000/user/hadoop/user_balance_table.csv", header=True, inferSchema=True)

    # 创建视图
    user_profile_df.createOrReplaceTempView("user_profiles")
    user_balance_df.createOrReplaceTempView("user_balances")

    # 执行SQL查询
    result = spark.sql("""
        SELECT p.city, AVG(b.tBalance) AS avg_balance
        FROM user_profiles p
        JOIN user_balances b ON p.user_id = b.user_id
        WHERE b.report_date = '20140301'
        GROUP BY p.city
        ORDER BY avg_balance DESC
    """)

    # 显示结果
    result.show()

    spark.stop()

if __name__ == "__main__":
    main()

