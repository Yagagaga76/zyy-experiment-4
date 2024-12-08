from pyspark import SparkContext, SparkConf

def parse_line(line):
    fields = line.split(',')
    # Check if the row is likely to be a header or improperly formatted
    try:
        report_date = fields[1]
        total_purchase_amt = int(fields[4])
        total_redeem_amt = int(fields[8])
        return (report_date, total_purchase_amt, total_redeem_amt)
    except ValueError:
        return None  # Return None for rows that cannot be processed

def main():
    # Create Spark config and context
    conf = SparkConf().setAppName("User Balance Analysis")
    sc = SparkContext(conf=conf)

    # Load data
    lines = sc.textFile("hdfs://localhost:9000/user/hadoop/user_balance_table.csv")
    
    # Parse data and filter out any None values from improper rows
    parsed_rdd = lines.map(parse_line).filter(lambda x: x is not None)

    # Compute total daily funds inflow and outflow
    daily_totals = parsed_rdd.map(lambda x: (x[0], (x[1], x[2])))\
                             .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))

    # Format results and print
    results = daily_totals.map(lambda x: f"{x[0]} {x[1][0]} {x[1][1]}")
    for result in results.collect():
        print(result)

    sc.stop()

if __name__ == "__main__":
    main()
