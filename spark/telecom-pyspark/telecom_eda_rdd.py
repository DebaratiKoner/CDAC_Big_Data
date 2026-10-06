from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, col


# 1. Initialize Spark


spark = SparkSession.builder \
    .appName("TelecomCustomerAnalytics") \
    .master("local[*]") \
    .getOrCreate()

print("Spark Started Successfully")



# 2. Load CSV Dataset


df = spark.read.csv(
   "file:///home/vboxuser/telecom-pyspark/data/telecom_customers.csv",
    header=True,
    inferSchema=True
)

print("\nOriginal Data:")
df.show()



# 3. Display Schema


print("\nOriginal Schema:")
df.printSchema()



# 4. Handle NULL Values


df_clean = df.fillna({
    "age": 0,
    "monthly_bill": 0.0,
    "tenure": 0
})

print("\nData After Handling NULL Values:")
df_clean.show()



# 5. Type Conversion


df_clean = df_clean \
    .withColumn("age", col("age").cast("integer")) \
    .withColumn("monthly_bill", col("monthly_bill").cast("double")) \
    .withColumn("tenure", col("tenure").cast("integer"))

print("\nSchema After Type Conversion:")
df_clean.printSchema()



# 6. EDA - Total Customers


total_customers = df_clean.count()

print("\nTotal Customers:")
print(total_customers)



# 7. EDA - Average Monthly Bill


print("\nAverage Monthly Bill:")

df_clean.select(
    avg("monthly_bill").alias("average_monthly_bill")
).show()



# 8. EDA - Customers By City


print("\nCustomers By City:")

df_clean.groupBy("city") \
    .agg(count("*").alias("customer_count")) \
    .orderBy(col("customer_count").desc()) \
    .show()



# 9. EDA - High Bill Customers


print("\nCustomers With Monthly Bill Greater Than 700:")

df_clean.filter(
    col("monthly_bill") > 700
).show()



# 10. EDA - Average Bill By Plan


print("\nAverage Monthly Bill By Plan:")

df_clean.groupBy("plan") \
    .agg(avg("monthly_bill").alias("average_bill")) \
    .show()



# 11. RDD Creation


rdd = spark.sparkContext.parallelize([100, 200, 300, 400, 500])

print("Original RDD:", rdd.collect())

# Map transformation
def double_value(x):
    return x * 2

rdd_doubled = rdd.map(double_value)
print("After map:", rdd_doubled.collect())

# Filter transformation
def greater_than_250(x):
    return x > 250

rdd_filtered = rdd.filter(greater_than_250)
print("After filter:", rdd_filtered.collect())

print("Count:", rdd_filtered.count())


# 14. RDD Action


print("RDD Count:")
print(rdd_filtered.count())



# 15. Cache Optimization


df_clean.cache()

print("\nFirst Action After Cache:")
start_count = df_clean.count()

print("Customer Count:")
print(start_count)


print("\nSecond Action After Cache:")
average_result = df_clean.select(
    avg("monthly_bill")
).collect()

print(average_result)



# 16. Stop Spark


spark.stop()

print("\nSpark Application Completed")
