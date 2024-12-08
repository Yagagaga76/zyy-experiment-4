from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum
from pyspark.sql.window import Window
from pyspark.sql.functions import rank

def main():
    spark = SparkSession.builder.appName("Top 3 Users by City").getOrCreate()

    # 加载数据
    user_profile_df = spark.read.csv("hdfs://localhost:9000/user/hadoop/user_profile_table.csv", header=True, inferSchema=True)
    user_balance_df = spark.read.csv("hdfs://localhost:9000/user/hadoop/user_balance_table.csv", header=True, inferSchema=True)

    # 创建视图
    user_profile_df.createOrReplaceTempView("user_profiles")
    user_balance_df.createOrReplaceTempView("user_balances")

    # SQL查询计算每个用户在2014年8月的总流量
    spark.sql("""
        SELECT p.city, b.user_id,
               (SUM(b.total_purchase_amt) + SUM(b.total_redeem_amt)) AS total_traffic
        FROM user_profiles p
        JOIN user_balances b ON p.user_id = b.user_id
        WHERE b.report_date BETWEEN '20140801' AND '20140831'
        GROUP BY p.city, b.user_id
    """).createOrReplaceTempView("city_user_traffic")

    # 使用窗口函数按城市分组对用户总流量进行排序，并获取每个城市的前三名用户
    windowSpec = Window.partitionBy("city").orderBy(col("total_traffic").desc())
    top_users_by_city = spark.sql("""
        SELECT city, user_id, total_traffic, RANK() OVER (PARTITION BY city ORDER BY total_traffic DESC) as rank
        FROM city_user_traffic
    """).filter("rank <= 3")

    # 显示结果
    top_users_by_city.show()

    spark.stop()

if __name__ == "__main__":
    main()

