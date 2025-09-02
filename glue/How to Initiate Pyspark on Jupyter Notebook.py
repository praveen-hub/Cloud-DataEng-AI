#These following commands are mandatory for running jupyter notebooks
============================================================================================

=> Goto Powershell and type "jupyter notebook"

=> Once notebook is launched in chrome type below and run in python kernel

import findspark
findspark.init()
findspark.find()

#Below is sample Code to run

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
    data = [('James', 'Smith', 'M', 30),
            ('Anna', 'Rose', '', 41),
            ('Robert', 'Williams', 'M', 62),
            ('James', 'Smith', 'F', 120),
            ('Anna', 'Rose', '', 4),
            ('Robert', 'Williams', 'F', 12)]
    rdd = sc.parallelize(data)
    columns = ["firstName","SecondName","Gender","Age"]
    df = rdd.toDF(schema=columns)
    df.show()


