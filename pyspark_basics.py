import os
import pyspark
from pyspark.sql import SparkSession

# Set the correct Python executable
os.environ['PYSPARK_PYTHON'] = 'python'
os.environ['PYSPARK_DRIVER_PYTHON'] = 'python'

print(os.environ.get("SPARK_HOME"))
print(os.environ.get("JAVA_HOME"))
print(os.environ.get("HADOOP_HOME"))


# Create a SparkSession
spark = SparkSession.builder.appName("FirstSteps").getOrCreate()
print(spark)

# # Example: Creating a simple DataFrame
data = [("Alice", 34), ("Bob", 45), ("Cathy", 29)]
columns = ["Name", "Age"]

df = spark.createDataFrame(data, columns)
df.show()

