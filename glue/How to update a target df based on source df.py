import pyspark.sql.functions as f
import sys
import os
from pyspark import SparkContext, SparkConf
from pyspark.sql import *
from pyspark.sql.functions import *

#Initiating Environment variiables
os.environ['PYSPARK_HOME'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# create Spark context with Spark configuration
conf = SparkConf().setAppName("Word Count - Python")
sc = SparkContext(conf=conf)
spark = SparkSession.builder.master("local[1]") \
    .appName("Phani df update").getOrCreate()

data1 = [(1,"2016-01-01",4650,22), (2,"2016-01-02",3130,45), (1,"2016-01-03",4456,22) ,(2,"2016-01-15",1234,45)]

columns = ["id_no","start_date","amount","days"]
df1 = spark.createDataFrame(data=data1, schema = columns)
df1.show()

'''
df1.alias('a').join(
    df2.alias('b'), ['id_no', 'start_date'], how='outer'
).select('id_no', 'start_date',
    f.coalesce('b.amount', 'a.amount').alias('amount'),
    f.coalesce('b.days', 'a.days').alias('days')
).show()
'''