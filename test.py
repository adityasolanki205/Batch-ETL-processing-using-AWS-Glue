import sys
import datetime

import json
from collections import Iterable, OrderedDict
from itertools import product
import logging

import boto3
import pyspark
from pyspark.sql import SparkSession
#import findspark
import pyspark.sql.functions as fn
from pyspark.sql import functions as sf
from pyspark.sql import Row
from pyspark.sql.column import Column, _to_java_column
from pyspark.sql.types import array, ArrayType, IntegerType, NullType, StringType, StructType
from pyspark.sql.functions import col, concat_ws, collect_list, explode, lit, split, when, upper

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame
from awsglue.transforms import Relationalize
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

appName = "testing"
master = "local"
spark = SparkSession.builder.\
        appName(appName).\
        master(master).\
        getOrCreate() 
        
date = datetime.datetime.now()
formatDate= (date.strftime("%Y%m%d"))

# client = boto3.client('s3')
# response = client.list_objects_v2(
#     Bucket="logtabletesting",
#     Prefix= formatDate + "/"
#     )
    
# for key in (response['Contents']):
#     completename= (key['Key'])

# filename = completename.split('/')[1]

df = spark.read.option( "inferSchema" , "true" ).option("header","true").csv("s3://inputgluetesting/german_data.csv")
raw_cnt  = df.count()


# df_log = spark.read.parquet("s3://logtabletesting/20230424/*") 
# print ("Input count at Raw: " + str(raw_cnt))

new_load_id = df_log.agg({"LoadID": "max"}).collect()[0][0] + 1

# df = df.filter((df.Purpose != 'NULL') & (df.Existing_account != 'NULL') & (df.Property !=  'NULL') & (df.Personal_status != 'NULL') & (df.Existing_account != 'NULL')  & (df.Credit_amount != 'NULL' ) & (df.Installment_plans != 'NULL'))

# # Changing the Datatype of Credit Amount from string to Float
# df = df.withColumn("Credit_amount", df['Credit_amount'].cast('float'))

# # Converting data into better readable format. Here Existing amount column is segregated into 2 columns Months and days
# split_col= pyspark.sql.functions.split(df['Existing_account'], '')
# df = df.withColumn('Month', split_col.getItem(0))
# df = df.withColumn('day1', split_col.getItem(1))
# df = df.withColumn('day2', split_col.getItem(2))

# df = df.withColumn('Days', sf.concat(sf.col('day1'),sf.col('day2')))

# # Converting data into better readable format. Here Purpose column is segregated into 2 columns File Month and Version
# split_purpose= pyspark.sql.functions.split(df['Purpose'], '')
# df = df.withColumn('File_month', split_purpose.getItem(0))
# df = df.withColumn('ver1', split_purpose.getItem(1))
# df = df.withColumn('ver2', split_purpose.getItem(2))

# df=df.withColumn('Version', sf.concat(sf.col('ver1'),sf.col('ver2')))

# Month_Dict = {
#     'A':'January',
#     'B':'February',
#     'C':'March',
#     'D':'April',
#     'E':'May',
#     'F':'June',
#     'G':'July',
#     'H':'August',
#     'I':'September',
#     'J':'October',
#     'K':'November',
#     'L':'December'
#     }

# df= df.replace(Month_Dict,subset=['File_month'])
# df = df.replace(Month_Dict,subset=['Month'])

# #Dropping unwanted columns from the dataframe.
# df = df.drop('day1')
# df = df.drop('day2')
# df = df.drop('ver1')
# df = df.drop('ver2')
# df = df.drop('Purpose')
# df = df.drop('Existing_account')
stage_cnt = df.count()
final_cnt = 0 
print ("Input count at stage: " + str(stage_cnt))


date = datetime.datetime.now()
formatDate= (date.strftime("%Y%m%d"))

loadDate = (date.strftime("%d-%m-%Y"))
dataf = [(new_load_id,loadDate, raw_cnt, stage_cnt,final_cnt )]
columns = ["loadid","loaddate","raw_count","stage_count","final_count"]
df_log = spark.createDataFrame(data=dataf, schema = columns)

gcsbucket_log = "s3://logtabletesting/" + formatDate + "/"
gcsbucket_output = "s3://outputgluetesting/" + formatDate + "/"
df.write.format("parquet").mode("append").option('header', 'true').save(gcsbucket_output)

df_log.write.format("parquet").mode("append").option('header', 'true').save(gcsbucket_log)

job.commit()

-----------------ETL------------------
import sys
import datetime

import json
from collections import Iterable, OrderedDict
from itertools import product
import logging

import boto3
import pyspark
from pyspark.sql import SparkSession
#import findspark
import pyspark.sql.functions as fn
from pyspark.sql import functions as sf
from pyspark.sql import Row
from pyspark.sql.column import Column, _to_java_column
from pyspark.sql.types import array, ArrayType, IntegerType, NullType, StringType, StructType
from pyspark.sql.functions import col, concat_ws, collect_list, explode, lit, split, when, upper

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame
from awsglue.transforms import Relationalize
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

appName = "testing"
master = "local"
spark = SparkSession.builder.\
        appName(appName).\
        master(master).\
        getOrCreate()     
date = datetime.datetime.now()
formatDate= (date.strftime("%Y%m%d"))


client = boto3.client('s3')
response = client.list_objects_v2(
    Bucket="logtabletesting",
    Prefix=formatDate + "/"
    )
log_bucketname = "s3://outputgluetesting/" + formatDate + "/*"
for key in (response['Contents']):
    completename= (key['Key'])

filename = completename.split('/')[1]

df = spark.read.parquet("s3://outputgluetesting/20230424/*")

gcsbucket_log = "s3://logtabletesting/" + formatDate + "/"

df_log = spark.read.parquet(gcsbucket_log + "*") 


load_id = df_log.collect()[0].loadid
raw_cnt = df_log.collect()[0].raw_count
stage_cnt  = df_log.collect()[0].stage_count
print ("Input count at stage: " + str(stage_cnt))


final_cnt = df.count()

print ("Input count at stage: " + str(final_cnt))


date = datetime.datetime.now()
formatDate= (date.strftime("%Y%m%d"))

#loadDate = (date.strftime("%d-%m-%Y"))
loadDate = df_log.collect()[0].loaddate

dataf = [(load_id,loadDate, raw_cnt, stage_cnt,final_cnt )]
columns = ["loadid","loaddate","raw_count","stage_count","final_count"]
df_log = spark.createDataFrame(data=dataf, schema = columns)
del_filename = formatDate + "/" + filename

gcsbucket_output = "s3://outputgluetesting/" + formatDate + "/"
print (df)
response = client.delete_object(Bucket="logtabletesting" ,  Key=del_filename)

df.write.format("parquet").mode("append").option('header', 'true').save(gcsbucket_output)

df_log.write.format("parquet").mode("append").option('header', 'true').save(gcsbucket_log)



job.commit()
-----------------------------------ETL ----------------------------------------

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
df_log = spark.read.parquet("s3://logtabletesting/20230424/*") 
df_log
raw_cnt = 0
stage_cnt = 0
final_cnt = 0
dataf = [(load_id,loadDate, raw_cnt, stage_cnt,final_cnt )]
columns = ["loadid","loaddate","raw_count","stage_count","final_count"]
df_log = spark.createDataFrame(data=dataf, schema = columns)
df_log = spark.read.parquet("s3://logtabletesting/20230424/*") 
#print ("Input count at stage: " + str(stage_cnt))
load_id = df_log.collect()[0].loadid
load_id
df_log.collect()
import boto3

s3_client = boto3.client("s3")
response = s3_client.delete_object(Bucket="logtabletesting" ,  Key="20230424/icici_credit_card_closing.txt")
print (response)
client = boto3.client('s3')
response = client.list_objects_v2(
    Bucket="logtabletesting",
    Prefix="20230424/"
    )
    
for key in (response['Contents']):
    completename= (key['Key'])

filename = completename.split('/')[1]
filename
del_filename = "20230424/" + filename
del_filename
df = spark.read.option( "inferSchema" , "true" ).option("header","true").csv("s3://glueoutputquery/logtablenew.csv")
df.write.format("parquet").mode("append").option('header', 'true').save("s3://outputgluetesting/dt20230421/")
import datetime
date = datetime.datetime.now()
formatDate= (date.strftime("%Y%m%d"))
client = boto3.client('s3')
response = client.list_objects_v2(
    Bucket="logtabletesting",
    Prefix= formatDate + "/"
    )
# log_bucketname = "s3://outputgluetesting/" + formatDate + "/*"
print (response)
for key in (response['Contents']):
    completename= (key['Key'])
completename
job.commit()




