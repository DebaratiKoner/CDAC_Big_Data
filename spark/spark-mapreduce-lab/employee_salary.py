from pyspark import SparkConf, SparkContext

# Create Spark configuration
conf = SparkConf() \
    .setAppName("EmployeeSalaryAnalysis") \
    .setMaster("local[*]")

# Create SparkContext
sc = SparkContext(conf=conf)

# Read input file from LOCAL filesystem
input_path = "file:///home/vboxuser/CDAC_Big_Data/pyspark/spark-mapreduce-lab/input/employees.csv"

lines = sc.textFile(input_path)

# Remove header
header = lines.first()
data = lines.filter(lambda line: line != header)

# MAP
# (department, (salary, employee_count))
mapped = data.map(
    lambda line: (
        line.split(",")[2],
        (int(line.split(",")[3]), 1)
    )
)

# REDUCE
# Add salary and employee count
reduced = mapped.reduceByKey(
    lambda x, y: (
        x[0] + y[0],
        x[1] + y[1]
    )
)

# Calculate average salary
result = reduced.mapValues(
    lambda x: (
        x[1],
        x[0],
        x[0] / x[1]
    )
)

# Sort by department
sorted_result = result.sortByKey()

# Display result
print("\nDepartment Salary Analysis")
print("----------------------------------------")

for department, values in sorted_result.collect():
    employee_count = values[0]
    total_salary = values[1]
    average_salary = values[2]

    print(
        department,
        employee_count,
        total_salary,
        average_salary
    )

# Prepare output
output = sorted_result.map(
    lambda x: (
        x[0],
        x[1][0],
        x[1][1],
        x[1][2]
    )
)

# Remove previous output if it exists
import os
import shutil

output_path = "/home/vboxuser/CDAC_Big_Data/pyspark/spark-mapreduce-lab/output/department_salary"

if os.path.exists(output_path):
    shutil.rmtree(output_path)

# Save output locally
output.map(
    lambda x: ",".join(map(str, x))
).coalesce(1).saveAsTextFile(
    "file://" + output_path
)

# Stop Spark
sc.stop()
