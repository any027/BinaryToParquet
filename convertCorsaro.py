from pyspark import SQLContext, SparkConf, SparkContext
from pyspark.sql.types import *
from pyspark.sql import Row

import numpy as np
import gzip
import zlib
import struct
import binascii
import sys
import socket

#setting the configurations for the SparkConf object here
conf = (SparkConf()
         .setMaster("local[4]")
         .setAppName("convertCorsaro.py")
        .set("spark.executor.memory", "1g"))

#creating the SparkConf object here
sc = SparkContext(conf = conf)

#creating the sqlContext that will be used
sqlContext = SQLContext(sc)





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






parquetCounter = 0
def toParquet(results):
	global parquetCounter
	print 'STARTING SPARK SCRIPT'
	#print results
	#after everything is done, parallelize the list and work with it?
	df = sqlContext.createDataFrame(sc.parallelize(results))
	print 'END SPARK SCRIPT'

	parquetNumber = str(parquetCounter)
	df.write.parquet('data/corstoparquet/')

	parquetCounter = parquetCounter + 1

#creating string format here?

#this creates the header format of: 
#magicnumber[32] intervalmagic[32] interval_number[16] interval_start_time[32]

#pretty nice header format atm
headerfmt = '!IIHI' 

results = []

#For this step, I will be reading gzip files and decompressing it
#Considering reading it line by line?


opener = gzip.open('python/net44.1320969600.flowtuple.cors.gz', 'r')

header = opener.read(14)

firstTime = 1
#print type(header)
#print len(header)

#LOOP 1
#keep looping while there are still stuff in the first interval
while( len(header) == 14):
#for x in xrange(0,2):
	#here we are unpacking the first interval

	#doing things the first time around
	if(firstTime == 1):
		headerPart = struct.unpack(headerfmt, header)
		#print 'first header'
		#print headerPart

		#this reads the flowtuple headerformat of 
		#magicnumber[32], classid[16], keycnt[32]
		ftheaderfmt = "!IHI"
		ftheader = opener.read(10)
		ftheaderPart = struct.unpack(ftheaderfmt, ftheader)
		#print 'top of the first flow tuple is '
		#print ftheaderPart

		#over here we grab the key count so we can get ready to key_cnt many times

		key_cnt = ftheaderPart[2]
		magicNumber = ftheaderPart[0]
		#print magicNumber
		#print key_cnt

		#1397315668
		#MAGIC NUMBER SIXT
		firstTime = 0

	#

	#LOOP 2
	while(magicNumber == 0x53495854):
	
	#while(magicNumber == 1397315668):
		#LOOP 3
		for count in xrange(0,key_cnt):
	
			#grabbing the destination ip
			dst_ip = "!BBB"

			flowtupleformatA = "!I"

			flowtuplereadA = opener.read(4)
			flowtuplePartA = struct.unpack(flowtupleformatA, flowtuplereadA)	

			# 0x00AABBCC
			# partA = Ox00
			# partB = Ox4E
			# partC = 0x3F
			# partD = 0x2B
			# result = A +( B << 16 ) + (C << 8) + D

			flowtupleformatB = "!BBB"
	
			flowtuplereadB = opener.read(3)
			flowtuplePartB = struct.unpack(flowtupleformatB, flowtuplereadB)

			partA = flowtuplePartB[0]
			partB = flowtuplePartB[1]
			partC = flowtuplePartB[2]

			#shifting everything so it would be like dst = 0x00AABBCC
			dst = (44 << 24 ) + ( partA << 16 ) + ( partB << 8) + partC
			#print dst
	
			flowtuplePartA = flowtuplePartA + (dst,)

			#print flowtuplePartA

			flowtupleformatC = "!HHBBBHI"

			flowtuplereadC = opener.read(13)
			flowtuplePartC = struct.unpack(flowtupleformatC, flowtuplereadC)

			#here we need to translate things from binary to ascii
			flowtuple = flowtuplePartA + flowtuplePartC

			flowtupleCol = Row(src_ip = flowtuple[0], dst_ip = flowtuple[1], src_port = flowtuple[2], dst_port = flowtuple[3], protocol = flowtuple[4], ttl = flowtuple[5], tcp_flags = flowtuple[6], ip_len = flowtuple[7], packet_cnt = flowtuple[8])
			#writing the flow tuple results into memory (currently doing a list)
			results.append(flowtupleCol)

			#flow1 = socket.inet_ntoa(struct.pack('!L',flowtuple[0]))
			#flow2 = socket.inet_ntoa(struct.pack('!L',flowtuple[1]))
			#flow3 = str(unicode(flowtuple[2]))
			#flow4 = str(unicode(flowtuple[3]))
			#flow5 = str(unicode(flowtuple[4]))
			#flow6 = str(unicode(flowtuple[5]))
			#flow7 = format(flowtuple[6], '#04x')
			#flow8 = str(unicode(flowtuple[7]))
			#flow9 = str(unicode(flowtuple[8]))

			#csvFormat = (flow1 + ',') + (flow2 + ',') + (flow3 + ',') + (flow4 + ',') + (flow5 + ',') + (flow6 + ',') + (flow7 + ',') +(flow8 + ',')+ (flow9) 
			#print csvFormat



		toParquet(results)
		results = []
		# <magic_number[32]><class_id[16]> 
		ftendfmt = "!IH"
		ftendRead = opener.read(6)
		ftendPart = struct.unpack(ftendfmt, ftendRead)

		#print 'CLASS ID IS CURRENTLY' 
		#print ftendPart[1]

		#print 'MAGIC NUMBER ENDS WITH'
		magicNumber = ftendPart[0]
		#print magicNumber

		#IF magicNumber = 0x45444752 WE ARE STARTING A NEW ONE

		#ELSE magicNumber = 0x53495854 WE ARE STILL DOING THE LOOP


		#IF = <magic_number[32]><interval_magic[32]><interval_number[16]><interval_end_time[32]>
		
		#ELSE = <magic_number[32]><class_id[16]><key_cnt[32]>



		# First, we check the magic number here.
		magicfmt = "!I"
		magicRead = opener.read(4)
		magicPart = struct.unpack(magicfmt, magicRead)
		#print 'HEADER MAGIC NUMBER IS'
		#print magicPart
		#We are comparing the magic number to EDGR

		if(magicPart[0] == 0x45444752):
			magicBufferfmt = "!IHI"
			magicBufferRead = opener.read(10)
			magicBufferPart = struct.unpack(magicBufferfmt, magicBufferRead)
			magicNumber = magicPart[0]
			key_cnt = 0
			#print 'NEW LOOP'

		#ELSE we are still in the loop, magic number should still be SIXT
		#or otherwise known as 0x53495854
		elif(magicPart[0] == 0x53495854):	
			# start of a new one here
			newfmt = "!HI"
			newRead = opener.read(6)
			newPart = struct.unpack(newfmt, newRead)
			magicNumber = magicPart[0]
			#print 'CONTINUE LOOP'
			key_cnt = newPart[1]

	
	##gotta do new things here

	#handling <magic_number[32]><interval_magic[32]><interval_number[16]><interval_start_time[32]>
	header = opener.read(14)
	if( len(header) != 14):
	#	print 'done with everything'
		exit()
	else:
		headerPart = struct.unpack(headerfmt, header)
	#	print 'NEW HEADER'
	#	print headerPart

		#this reads the flowtuple headerformat of 
		#magicnumber[32], classid[16], keycnt[32]
		ftheaderfmt = "!IHI"
		ftheader = opener.read(10)
		ftheaderPart = struct.unpack(ftheaderfmt, ftheader)
	#	print 'top of the new flow tuple is '
	#	print ftheaderPart
		key_cnt = ftheaderPart[2]
		magicNumber = ftheaderPart[0]



