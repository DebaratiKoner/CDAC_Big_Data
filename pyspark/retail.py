from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("Retail Analysis").getOrCreate()
data=spark.read.csv("sales.csv",header=True,inferSchema=True)
data.show()
clean_data=data.dropna()
clean_data
clean_data.show()
clean_data.printSchema()
from pyspark.sql.functions import sum
result=clean_data.groupBy("product").agg(sum("amount").alias("total_sales"))
result.show()
result.write.csv("output",header=True)
data.printSchema()
spark.stop()
#stream_df=spark.readStream.csv("stream_folder",header=True)
#stream_df.writeStream.format("console").start().awaitTermination()
