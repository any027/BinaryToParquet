from pyspark import SQLContext, SparkConf, SparkContext
from pyspark.sql.types import *
import numpy as np

#code is based off of 
#https://developer.ibm.com/hadoop/2015/12/03/parquet-for-spark-sql/

#setting the configurations for the SparkConf object here
conf = (SparkConf()
         .setMaster("local[4]")
         .setAppName("convert.py")
        .set("spark.executor.memory", "1g"))

#creating the SparkConf object here
sc = SparkContext(conf = conf)

#creating the sqlContext that will be used
sqlContext = SQLContext(sc)

#creating custom schema otherwise known as flowtuple here
customSchema = StructType([ 
	StructField('src_ip', StringType(), True), 
	StructField('dst_ip', StringType(), True), 
	StructField('src_port', IntegerType(), True), 
	StructField('dst_port', IntegerType(), True), 
	StructField('protocol', IntegerType(), True), 
	StructField('ttl', IntegerType(), True), 
	StructField('tcp_flags', StringType(), True), 
	StructField('ip_len', IntegerType(), True), 
	StructField('packet_cnt', IntegerType(), True) ] )
# 
# sql = SQLContext
# fileName = String 
# schema = StructType,
# tableName = String

#writing the convert method here
def convert(sqlContext, fileName, schema, tableName):
	df = sqlContext.read.format('com.databricks.spark.csv').schema(schema).options(delimiter=',',nullValue='',treatEmptyValuesAsNulls='true').load(fileName)
	df.write.parquet("data/"+tableName)

#calling the method with the SQLcontext, the csv file, the schema, into 'result'
convert(sqlContext, 'python/net44.1320969600.flowtuple.csv', customSchema,'test2') 
