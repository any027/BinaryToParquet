from pyspark import SQLContext, SparkConf, SparkContext
from pyspark.sql.types import *
import numpy as np


#setting the configurations for the SparkConf object here
conf = (SparkConf()
         .setMaster("local[4]")
         .setAppName("convert.py")
        .set("spark.executor.memory", "1g"))

#creating the SparkConf object here
sc = SparkContext(conf = conf)

#creating the sqlContext that will be used
sqlContext = SQLContext(sc)

#reading the parquet file?
parquetFile = sqlContext.read.parquet('data/test2') 
parquetFile.registerTempTable("parquetFile")

base = "SELECT * FROM parquetFile WHERE"
command = ' ip_len >= 1500'
test = sqlContext.sql(base + command)
print test.show()
