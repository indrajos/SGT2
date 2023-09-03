import jdk
from jdk.enums import OperatingSystem, Architecture
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum
from pyspark.sql.functions import split
from datetime import datetime, timedelta
from collections import defaultdict

# 1. **Data Aggregation:**
# Read a dataset containing sales transactions.
# Calculate the total sales amount for each product category using Spark's `groupBy` and aggregation functions.


spark = SparkSession.builder.appName("SparkTasks").getOrCreate()

df = spark.read.option("sep", ",").csv("sales.csv", header=True, inferSchema=True)

df.createOrReplaceTempView("sales")

query1 = """SELECT Category,sum(Amount) AS total_amount FROM sales
  GROUP BY Category"""

result_1 = spark.sql(query1)
result_1.show()

spark.sparkContext.stop()

# 2. **Log Analysis:**
# Analyze server log data to find the most frequently accessed URLs and their corresponding IP addresses.
# Use Spark SQL to query and visualize the results.

spark = SparkSession.builder.appName("LogAnalysis").getOrCreate()

log_data = spark.read.text("server_log.txt")

log_df = log_data.select(
    split(log_data["value"], " ")[0].alias("date"),
    split(log_data["value"], " ")[1].alias("time"),
    split(log_data["value"], " ")[2].alias("url"),
    split(log_data["value"], " ")[3].alias("ip")
)

log_df.createOrReplaceTempView("logs")

most_visited_url = spark.sql("SELECT url, COUNT(*) AS cnt FROM logs GROUP BY url ORDER BY cnt DESC LIMIT 1")

url_value = most_visited_url.select("url").first()[0]

ips_of_most_visited_url = spark.sql(f"SELECT ip FROM logs WHERE url = '{url_value}'")
ips_of_most_visited_url.show()

spark.sparkContext.stop()

# MapReduce Tasks:
# 1. **URL Access Count:**
# Given a log file containing records of URLs accessed and their corresponding timestamps,
# use MapReduce to count the number of times each URL was accessed within a specific time window.


with open("access_log.txt", "r") as f:
    Lines = f.readlines()

start_time = datetime.strptime("2023-08-01 10:00:00", "%Y-%m-%d %H:%M:%S")
end_time = start_time + timedelta(hours=1)

url_counts = defaultdict(int)

for line in Lines:
        parts = line.strip().split(" ", 2)
        timestamp_str, tm, url = parts
        ts = ' '.join(parts[0:2])
        timestamp = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
        if start_time <= timestamp <= end_time:
            url_counts[url] += 1

print(url_counts.items())


# 2. **Follower Recommendations:**
# Given a dataset representing a social network's following graph.
# Use MapReduce to recommend the users to follow for another users who do have a mutual followers,
# but do not follow each other.

with open("follower_graph.txt", "r") as file:
    lines = file.readlines()

followers_dict = {}

# Populate the followers dictionary
for line in lines:
    parts = line.strip().split()
    user = parts[0]
    followers = set(parts[1:])
    followers_dict[user] = followers

# Create a set to store mutual followers recommendations
recommendations = set()

# Find mutual followers recommendations
for user1, followers1 in followers_dict.items():
    for user2, followers2 in followers_dict.items():
        if user1 != user2:
            mutual_followers = followers1 & followers2
            if len(mutual_followers) > 0 and user1 not in followers2 and user2 not in followers1:
                recommendations.add((user1, user2))

# Print the recommendations
for recommendation in recommendations:
    print(f"Recommend {recommendation[0]} to follow {recommendation[1]}")
