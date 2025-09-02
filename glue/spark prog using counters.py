# Importing all needed modules
import sys
import os
from pyspark import SparkContext, SparkConf
from pyspark.sql import *
from pyspark.sql.functions import *
from collections import Counter

# Initiating Environment variiables
os.environ['PYSPARK_HOME'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Main Block
if __name__ == "__main__":
    # create Spark context with Spark configuration
    conf = SparkConf().setAppName("Word Count - Python")
    sc = SparkContext(conf=conf)
    spark = SparkSession.builder.master("local[1]") \
        .appName("Phani df test").getOrCreate()

    # Core Logic
    data = [("James", "Smith", "M", 30),
            ("Anna", "Rose","" , 41),
            ("Robert", "Williams", "M",""),
            ("James", "Smith", "F", 120),
            ("Anna", "Rose", None , 4),
            ("Robert", "Williams", "F", 12)]
    rdd = sc.parallelize(data)
    columns = ["firstName","SecondName","Gender","Age"]
    df = rdd.toDF(schema=columns)
    #df.show()

    def counters(x):
        fname = x.firstName
        sname = x.SecondName
        gender = x.Gender
        age = x.Age
        result = Counter(fname)
        print("result is ",result)
        return result

    rdd1 = df.rdd.map(lambda x : counters(x))
    for i in rdd1.collect(): print(i)





