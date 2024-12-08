# zyy-experiment-4
### 实验四

##### spark安装

- 首先从http://spark.apache.org/downloads.html下载了3.5.3的spark
- 解压spark文件

```
cd /usr/local #我是讲spark放在了这个文件下
sudo tar -xzvf spark-3.5.3-bin-hadoop3.tgz
sudo mv spark-3.3.0-bin-hadoop3 spark
```

- 设置环境变量

```
nano ~/.bashrc
export SPARK_HOME=/usr/local/spark
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
source ~/.bashrc
```

- 配置Spark以确保其能与Hadoop集成

```
sudo cp $SPARK_HOME/conf/spark-env.sh.template $SPARK_HOME/conf/spark-env.sh
sudo cp $SPARK_HOME/conf/spark-defaults.conf.template $SPARK_HOME/conf/spark-defaults.conf
```

编辑`spark-env.sh`，添加Hadoop配置目录和Spark master主机地址：

```
sudo nano $SPARK_HOME/conf/spark-env.sh
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export SPARK_MASTER_HOST=localhost
```

- 启动Spark的Master和一个Worker：

```
start-master.sh
start-worker.sh spark://localhost:7077
```

- 在浏览器中访问`http://localhost:8080`，查看Spark Master的Web界面以确认Spark集群状态。

<img src="D:\大三上\大数据\实验\实验四\安装.png" alt="安装" style="zoom: 33%;" />

- 运行一个测试作业，如计算π的值，验证安装：

```
$SPARK_HOME/bin/run-example SparkPi 10
```

<img src="D:\大三上\大数据\实验\实验四\验证.png" alt="验证" style="zoom:67%;" />

可以看到成功计算出结果，成功启动。

##### 任务**1**：**Spark RDD**编程

**1、查询特定⽇期的资金流入和流出情况： 使⽤ user_balance_table ，计算出所有⽤户在每⼀天的总资⾦流⼊和总资⾦流出量。**

```bash
#创建一个新目录来存放数据和代码
mkdir /usr/local/spark_work
mv ~/Downloads/user_balance_table.csv /usr/local/spark_work/
```

在`/usr/local/spark_work`目录中，创建一个新的Python脚本文件

```bash
cd /usr/local/spark_work
sudo nano /usr/local/spark_work/user_balance_analysis.py
```

使用`spark-submit`命令来运行Spark脚本：

```
spark-submit user_balance_analysis.py
```

代码：

```python
from pyspark import SparkContext, SparkConf

def parse_line(line):
    fields = line.split(',')
    try:
        report_date = fields[1]
        total_purchase_amt = int(fields[4])
        total_redeem_amt = int(fields[8])
        return (report_date, total_purchase_amt, total_redeem_amt)
    except ValueError:
        return None 

def main():
    conf = SparkConf().setAppName("User Balance Analysis")
    sc = SparkContext(conf=conf)
    lines = sc.textFile("hdfs://localhost:9000/user/hadoop/user_balance_table.csv")
    parsed_rdd = lines.map(parse_line).filter(lambda x: x is not None)
    daily_totals = parsed_rdd.map(lambda x: (x[0], (x[1], x[2])))\
                             .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))


    results = daily_totals.map(lambda x: f"{x[0]} {x[1][0]} {x[1][1]}")
    for result in results.collect():
        print(result)

    sc.stop()

if __name__ == "__main__":
    main()

```

- 使用 `parse_line` 函数解析每一行数据，提取 `report_date`（报告日期），`total_purchase_amt`（总购买金额），和 `total_redeem_amt`（总赎回金额）。如果转换过程中出现错误（例如数据格式不正确），该行数据将被过滤掉。
- 将解析后的数据映射成键值对形式，键是 `report_date`，值是一个包含 `total_purchase_amt` 和 `total_redeem_amt` 的元组。
- 使用 `reduceByKey` 方法对同一天的数据进行聚合，计算每天的总购买和总赎回金额。

运行结果：

<img src="D:\大三上\大数据\实验\实验四\1.png" alt="1" style="zoom:50%;" />

2. **活跃用户分析： 使用 user_balance_table ，定义活跃用户为在指定月份内有至少五天记录的用户，统计2014年8月的活跃用户总数。**

代码：

```python
from pyspark import SparkContext, SparkConf

def parse_line(line):
    fields = line.split(',')
    try:
        user_id = fields[0]
        report_date = fields[1]
        if '201408' in report_date:
            return (user_id, report_date)
    except IndexError:
        return None
    return None

def main():
    conf = SparkConf().setAppName("Active User Analysis")
    sc = SparkContext(conf=conf)

    lines = sc.textFile("hdfs://localhost:9000/user/hadoop/user_balance_table.csv")
    
    user_dates = lines.map(parse_line).filter(lambda x: x is not None)
    user_unique_dates = user_dates.distinct().map(lambda x: (x[0], {x[1]}))
    user_aggregated_dates = user_unique_dates.reduceByKey(lambda a, b: a.union(b))
    active_users = user_aggregated_dates.filter(lambda x: len(x[1]) >= 5)
    active_user_count = active_users.count()
    print(f"Active users total: {active_user_count}")

    sc.stop()

if __name__ == "__main__":
    main()

```

- `parse_line(line)`函数尝试解析文本，将其分割为字段，并检查日期字段是否包含"201408"。如果是，它返回一个元组，包含用户ID和报告日期。如果行不能正确解析或日期不匹配，函数返回`None`。
- `filter(lambda x: x is not None)` 移除所有 `None` 值，这些通常是解析失败或日期不符的行。
- `distinct()` 去除重复的 (user_id, report_date) 对。
- `map(lambda x: (x[0], {x[1]}))` 转换为键值对，其中键是 `user_id`，值是包含一个日期的集合。

运行结果：

<img src="D:\大三上\大数据\实验\实验四\2.png" alt="2" style="zoom:50%;" />

##### 任务**2**：**Spark SQL**编程

**1、按城市统计2014年3⽉1⽇的平均余额： 计算每个城市在2014年3⽉1⽇的⽤户平均余额( tBalance )，按平均余额降序排列。**

代码：

```python
from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder.appName("Average Balance by City").getOrCreate()

    user_profile_df = spark.read.csv("hdfs://localhost:9000/user/hadoop/user_profile_table.csv", header=True, inferSchema=True)
    user_balance_df = spark.read.csv("hdfs://localhost:9000/user/hadoop/user_balance_table.csv", header=True, inferSchema=True)
    #将DataFrame注册为临时SQL视图，允许我们像操作SQL数据库表一样操作这些DataFrame。
    user_profile_df.createOrReplaceTempView("user_profiles")
    user_balance_df.createOrReplaceTempView("user_balances")


    result = spark.sql("""
        SELECT p.city, AVG(b.tBalance) AS avg_balance
        FROM user_profiles p
        JOIN user_balances b ON p.user_id = b.user_id
        WHERE b.report_date = '20140301'
        GROUP BY p.city
        ORDER BY avg_balance DESC
    """)


    result.show()

    spark.stop()

if __name__ == "__main__":
    main()

```

代码解释：

- 使用`JOIN`将`user_profiles`和`user_balances`表通过`user_id`字段联接。
- 通过`WHERE`语句筛选`report_date`为2014年3月1日的记录。
- 按`city`分组，并计算每个城市的平均余额`AVG(b.tBalance)`。
- 结果按平均余额降序排列。

运行结果:

![3](D:\大三上\大数据\实验\实验四\3.png)

**2、统计每个城市总流量前3⾼的⽤户：统计每个城市中每个⽤户在2014年8⽉的总流量（定义为total_purchase_amt + total_redeem_amt ），并输出每个城市总流量排名前三的⽤户ID及其总流量。**

代码：

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum
from pyspark.sql.window import Window
from pyspark.sql.functions import rank

def main():
    spark = SparkSession.builder.appName("Top 3 Users by City").getOrCreate()

    user_profile_df = spark.read.csv("hdfs://localhost:9000/user/hadoop/user_profile_table.csv", header=True, inferSchema=True)
    user_balance_df = spark.read.csv("hdfs://localhost:9000/user/hadoop/user_balance_table.csv", header=True, inferSchema=True)

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

```

代码解释：

- 使用`Window.partitionBy("city").orderBy(col("total_traffic").desc())`定义了一个按城市分组，根据总流量降序排序的窗口。

- 在查询中，对每个城市的用户总流量使用了`rank()`函数进行排名，并通过过滤条件选择排名前三的记录。
- 首先联结用户档案表和余额表，过滤出2014年8月的数据，计算总流量。

运行结果：

<img src="D:\大三上\大数据\实验\实验四\4.png" alt="4" style="zoom:50%;" />

#### 遇到的问题：

1. 读取文件

 Spark 尝试从 HDFS 读取数据文件 `user_balance_table.csv`，但没有在指定的路径找到该文件：

```
org.apache.hadoop.mapred.InvalidInputException: Input path does not exist: hdfs://localhost:9000/usr/local/spark_work/user_balance_table.csv
```

所以需要将文件上传到HDFS,具体操作同实验一。





