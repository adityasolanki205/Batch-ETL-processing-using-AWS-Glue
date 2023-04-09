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

bucket = "input-etl-glue"
spark.conf.set('temporaryGcsBucket', bucket)
df = spark.read.option( "inferSchema" , "true" ).option("header","true").csv("s3://inputgluetesting/german_data.csv")

df = df.filter((df.Purpose != 'NULL') & (df.Existing_account != 'NULL') & (df.Property !=  'NULL') & (df.Personal_status != 'NULL') & (df.Existing_account != 'NULL')  & (df.Credit_amount != 'NULL' ) & (df.Installment_plans != 'NULL'))

# Changing the Datatype of Credit Amount from string to Float
df = df.withColumn("Credit_amount", df['Credit_amount'].cast('float'))

# Converting data into better readable format. Here Existing amount column is segregated into 2 columns Months and days
split_col= pyspark.sql.functions.split(df['Existing_account'], '')
df = df.withColumn('Month', split_col.getItem(0))
df = df.withColumn('day1', split_col.getItem(1))
df = df.withColumn('day2', split_col.getItem(2))

df = df.withColumn('Days', sf.concat(sf.col('day1'),sf.col('day2')))

# Converting data into better readable format. Here Purpose column is segregated into 2 columns File Month and Version
split_purpose= pyspark.sql.functions.split(df['Purpose'], '')
df = df.withColumn('File_month', split_purpose.getItem(0))
df = df.withColumn('ver1', split_purpose.getItem(1))
df = df.withColumn('ver2', split_purpose.getItem(2))

df=df.withColumn('Version', sf.concat(sf.col('ver1'),sf.col('ver2')))

Month_Dict = {
    'A':'January',
    'B':'February',
    'C':'March',
    'D':'April',
    'E':'May',
    'F':'June',
    'G':'July',
    'H':'August',
    'I':'September',
    'J':'October',
    'K':'November',
    'L':'December'
    }

df= df.replace(Month_Dict,subset=['File_month'])
df = df.replace(Month_Dict,subset=['Month'])

#Dropping unwanted columns from the dataframe.
df = df.drop('day1')
df = df.drop('day2')
df = df.drop('ver1')
df = df.drop('ver2')
df = df.drop('Purpose')
df = df.drop('Existing_account')

df.write.format("csv").mode("append").option('header', 'true').save("s3://outputgluetesting/")


job.commit()

