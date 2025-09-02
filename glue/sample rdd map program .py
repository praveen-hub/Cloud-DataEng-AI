#Importing all needed modules 
import sys
import os 
from pyspark import SparkContext, SparkConf
from pyspark.sql import *
from pyspark.sql.functions import *

#Initiating Environment variiables
os.environ['PYSPARK_HOME'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

#Main Block 
if __name__ == "__main__":
 
  # create Spark context with Spark configuration
  conf = SparkConf().setAppName("Word Count - Python")
  sc = SparkContext(conf=conf)
  spark = SparkSession.builder.master("local[1]") \
    .appName("Phani df test").getOrCreate()
  
  #Core Logic
  data = [('James','Smith','M',30),
  ('Anna','Rose','F',41),
  ('Robert','Williams','M',62)]

columns = ["firstname","lastname","gender","salary"]
df = spark.createDataFrame(data=data, schema = columns)
df.show()

rdd = df.rdd.map(lambda x : valuefun(x))

def valuefun(x):
  firstName=x.firstname
  lastName=x.lastname
  name=firstName+","+lastName
  gender=x.gender.lower()
  salary=x.salary

  if salary == 41:
    salary = 22

  return (name,gender,salary)

for i in rdd.collect() : print(i)
  
  