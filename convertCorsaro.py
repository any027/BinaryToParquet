from pyspark import SQLContext, SparkConf, SparkContext
from pyspark.sql.types import *
from pyspark.sql import Row

import gzip
import zlib
import struct
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

parquetCounter = 0
def toParquet(results):
	global parquetCounter
	
	
	#after everything is done, parallelize the list and work with it

	df = sqlContext.createDataFrame(sc.parallelize(results))

	parquetNumber = str(parquetCounter)

	#Choose the destination for the parquet file here.
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


#Change this line to read whatever file you need to.
opener = gzip.open('python/net44.1320969600.flowtuple.cors.gz', 'r')

header = opener.read(14)

firstTime = 1

#OUTER LOOP HERE
#keep looping while there are still stuff in the first interval
while( len(header) == 14):
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

		#over here we grab the key count so we can get ready to key_cnt many times

		key_cnt = ftheaderPart[2]
		magicNumber = ftheaderPart[0]


	#INNER LOOP HERE
	while(magicNumber == 0x53495854):
	
		#INNER MOST LOOP HERE
		for count in xrange(0,key_cnt):
	
			#grabbing the destination ip
			dst_ip = "!BBB"

			flowtupleformatA = "!I"

			flowtuplereadA = opener.read(4)
			flowtuplePartA = struct.unpack(flowtupleformatA, flowtuplereadA)	

			# 0x00AABBCC
			# partA = Ox00
			# partB = Ox4E
			# partC
			# partD = 0x2B
			# result = A +( B << 16 ) + (C << 8) + D

			flowtupleformatB = "!BBB"
	
			flowtuplereadB = opener.read(3)
			flowtuplePartB = struct.unpack(flowtupleformatB, flowtuplereadB)

			partA = flowtuplePartB[0]
			partB = flowtuplePartB[1]
			partC = flowtuplePartB[2]

			#shifting everything so it would be like dst = 0x44AABBCC
			dst = (44 << 24 ) + ( partA << 16 ) + ( partB << 8) + partC

	
			flowtuplePartA = flowtuplePartA + (dst,)


			flowtupleformatC = "!HHBBBHI"

			flowtuplereadC = opener.read(13)
			flowtuplePartC = struct.unpack(flowtupleformatC, flowtuplereadC)

			#here we combine the tuples together.
			flowtuple = flowtuplePartA + flowtuplePartC

			#create a row for each binary variable to be put into a list.

			flowtupleCol = Row(src_ip = flowtuple[0], dst_ip = flowtuple[1], src_port = flowtuple[2], dst_port = flowtuple[3], protocol = flowtuple[4], ttl = flowtuple[5], tcp_flags = flowtuple[6], ip_len = flowtuple[7], packet_cnt = flowtuple[8])
			#writing the flow tuple results into memory (currently doing a list)
			results.append(flowtupleCol)


		toParquet(results)
		results = []

		# <magic_number[32]><class_id[16]> 
		ftendfmt = "!IH"
		ftendRead = opener.read(6)
		ftendPart = struct.unpack(ftendfmt, ftendRead)

		magicNumber = ftendPart[0]

		#IF magicNumber = 0x45444752 WE ARE STARTING A NEW ONE

		#ELSE magicNumber = 0x53495854 WE ARE STILL DOING THE LOOP


		#IF = <magic_number[32]><interval_magic[32]><interval_number[16]><interval_end_time[32]>
		
		#ELSE = <magic_number[32]><class_id[16]><key_cnt[32]>



		# First, we check the magic number here.
		magicfmt = "!I"
		magicRead = opener.read(4)
		magicPart = struct.unpack(magicfmt, magicRead)

		#We are comparing the magic number to EDGR

		if(magicPart[0] == 0x45444752):
			magicBufferfmt = "!IHI"
			magicBufferRead = opener.read(10)
			magicBufferPart = struct.unpack(magicBufferfmt, magicBufferRead)
			magicNumber = magicPart[0]
			key_cnt = 0

		#ELSE we are still in the loop, magic number should still be SIXT
		#or otherwise known as 0x53495854
		elif(magicPart[0] == 0x53495854):	
			# start of a new one here
			newfmt = "!HI"
			newRead = opener.read(6)
			newPart = struct.unpack(newfmt, newRead)
			magicNumber = magicPart[0]
			key_cnt = newPart[1]

	
	##Here, we handle the new iterations of the binary file.

	#handling <magic_number[32]><interval_magic[32]><interval_number[16]><interval_start_time[32]>
	header = opener.read(14)
	if( len(header) != 14):
		exit()
	else:
		headerPart = struct.unpack(headerfmt, header)

		#this reads the flowtuple headerformat of 
		#magicnumber[32], classid[16], keycnt[32]
		ftheaderfmt = "!IHI"
		ftheader = opener.read(10)
		ftheaderPart = struct.unpack(ftheaderfmt, ftheader)
		key_cnt = ftheaderPart[2]
		magicNumber = ftheaderPart[0]



