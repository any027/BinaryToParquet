from pyspark import SQLContext, SparkConf, SparkContext
from pyspark.sql.types import *

#setting the configurations for the SparkConf object here
conf = (SparkConf()
         .setMaster("local[4]")
         .setAppName("convert.py")
        .set("spark.executor.memory", "1g"))

#creating the SparkConf object here
sc = SparkContext(conf = conf)

#creating the sqlContext that will be used
sqlContext = SQLContext(sc)

#reading the parquet file

#Change this line to be the directory where the parquet file exists
parquetFile = sqlContext.read.parquet('data/test2') 


parquetFile.registerTempTable("parquetFile")

#Queries are made from the base + command.

#base SELECTS elements of what you are interested from WHERE 
base = "SELECT * FROM parquetFile WHERE"

#command is the query you make.
command = ' ip_len >= 1500'
test = sqlContext.sql(base + command)
print test.show()
