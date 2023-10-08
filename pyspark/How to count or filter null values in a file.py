# Importing all needed modules
import sys
import os
from pyspark import SparkContext, SparkConf
from pyspark.sql import *
from pyspark.sql.functions import *

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

    #To get the count of number of null values in all columns
    df2 = df.select([count(when(col(c).contains('None') | \
                          col(c).contains('Null') | \
                          (col(c) == '') | \
                          col(c).isNull() | \
                          isnan(c), c)).alias(c)
               for c in df.columns])
    #df2.show()

    #To select null values in two columns
    df3 = df.filter(df.Gender.isNull() & df.Age.isNull())
    df4 = df.filter(df.Gender.isNull() | df.Age.contains('Null'))
    df4.show()
    #df5 = df.filter(df.Gender.isNull() | df.Age.isNull())
    #df5.show()



