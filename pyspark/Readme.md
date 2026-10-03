export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH=/usr/lib/jvm/java-17-openjdk-amd64/bin:$PATH
hash -r
(pyspark_env) vboxuser@Ubuntu2:~/CDAC_Big_Data/PySpark$ java -version
openjdk version "17.0.20.1" 2026-08-18
OpenJDK Runtime Environment (build 17.0.20.1+1-1-26.04-Ubuntu)
OpenJDK 64-Bit Server VM (build 17.0.20.1+1-1-26.04-Ubuntu, mixed mode, sharing)
(pyspark_env) vboxuser@Ubuntu2:~/CDAC_Big_Data/PySpark$ echo $JAVA_HOME
/usr/lib/jvm/java-17-openjdk-amd64
(pyspark_env) vboxuser@Ubuntu2:~/CDAC_Big_Data/PySpark$ pyspark
(pyspark_env) vboxuser@Ubuntu2:~/CDAC_Big_Data/PySpark$ nano sales.csv
>>> from pyspark.sql import SparkSession
>>> spark = SparkSession.builder.appName("RetailAnalysis").getOrCreate()
>>> data = spark.read.csv("file:///home/vboxuser/CDAC_Big_Data/PySpark/sales.csv", header=True, inferSchema=\
True)
>>> data.show()
+-----------+-------+------+---------+
|customer_id|product|amount|     city|
+-----------+-------+------+---------+
|        101| Laptop|   800|Bangalore|
|        102|  Phone|   500|   Mumbai|
|        103| Laptop|   800|    Delhi|
|        104| Tablet|   300|Bangalore|
|        105|  Phone|   500|   Mumbai|
|        106| Laptop|   800|Bangalore|
+-----------+-------+------+---------+

>>> clean_data = data.dropna()
>>> 
... clean_data
... clean_data.show()  
+-----------+-------+------+---------+
|customer_id|product|amount|     city|
+-----------+-------+------+---------+
|        101| Laptop|   800|Bangalore|
|        102|  Phone|   500|   Mumbai|
|        103| Laptop|   800|    Delhi|
|        104| Tablet|   300|Bangalore|
|        105|  Phone|   500|   Mumbai|
|        106| Laptop|   800|Bangalore|
+-----------+-------+------+---------+

>>> clean_data.printSchema()
root
 |-- customer_id: integer (nullable = true)
 |-- product: string (nullable = true)
 |-- amount: integer (nullable = true)
 |-- city: string (nullable = true)

>>> from pyspark.sql.functions import sum
... result = clean_data.groupBy("product").agg(sum("amount").alias("total_sales"))
... result.show()
+-------+-----------+
|product|total_sales|
+-------+-----------+
|  Phone|       1000|
| Laptop|       2400|
| Tablet|        300|
+-------+-----------+

>>> result.write.csv(
...     "file:///home/vboxuser/CDAC_Big_Data/PySpark/output",
...     header=True
... )
>>> data.printSchema()
root
 |-- customer_id: integer (nullable = true)
 |-- product: string (nullable = true)
 |-- amount: integer (nullable = true)
 |-- city: string (nullable = true)

>>> spark.stop()
>>> stream_df = spark.readStream.csv("stream_folder", header=True)
... stream_df.writeStream.format("console").start().awaitTermination()
