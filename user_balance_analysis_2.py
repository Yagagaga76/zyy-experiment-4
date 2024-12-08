from pyspark import SparkContext, SparkConf

def parse_line(line):
    fields = line.split(',')
    try:
        user_id = fields[0]
        report_date = fields[1]
        # Ensure we are only looking at August 2014
        if '201408' in report_date:
            return (user_id, report_date)
    except IndexError:
        return None
    return None

def main():
    # Create Spark config and context
    conf = SparkConf().setAppName("Active User Analysis")
    sc = SparkContext(conf=conf)

    # Load data
    lines = sc.textFile("hdfs://localhost:9000/user/hadoop/user_balance_table.csv")
    
    # Parse data and filter out None values
    user_dates = lines.map(parse_line).filter(lambda x: x is not None)

    # Map to (user_id, set of unique dates)
    user_unique_dates = user_dates.distinct().map(lambda x: (x[0], {x[1]}))

    # Reduce by key to aggregate all unique dates for each user
    user_aggregated_dates = user_unique_dates.reduceByKey(lambda a, b: a.union(b))

    # Filter users who have records for at least 5 days in August 2014
    active_users = user_aggregated_dates.filter(lambda x: len(x[1]) >= 5)

    # Count active users
    active_user_count = active_users.count()

    # Print the result
    print(f"Active users total: {active_user_count}")

    sc.stop()

if __name__ == "__main__":
    main()

