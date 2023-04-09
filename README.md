# ETL batch Processing using AWS Glue
This is the part of **ETL processing in AWS** Repository. Here we will try to learn basics of Apache Spark to create **Batch** jobs on AWS Glue. In this repo We will learn step by step process to create a batch job using [German Credit Risk](https://www.kaggle.com/uciml/german-credit). The complete process is divided into 6 parts:

1. **Creating S3 buckets**
2. **Creating an IAM Role**
3. **Creating a Database**
4. **Creating a Glue Job**
5. **Creating a Crawler using Glue Crawler**
6. **Querying data in Athena**


## Motivation
For the last few years, I have been part of a great learning curve wherein I have upskilled myself to move into a Machine Learning and Cloud Computing. This project was practice project for all the learnings I have had. This is first of the many more to come. 
 

## Libraries/frameworks used

<b>Built with</b>
- [Apache Spark](https://spark.apache.org/)
- [Anaconda](https://www.anaconda.com/)
- [Python](https://www.python.org/)
- [AWS Glue](https://docs.aws.amazon.com/glue/latest/ug/what-is-glue-studio.html)
- [AWS S3](https://docs.aws.amazon.com/AmazonS3/latest/userguide/GetStartedWithS3.html)
- [AWS Athena](https://docs.aws.amazon.com/athena/latest/ug/what-is.html)

## Cloning Repository

```bash
    # clone this repo:
    git clone https://github.com/adityasolanki205/Batch-ETL-processing-using-AWS-Glue.git
```

## Job Construction

Below are the steps to setup the enviroment and run the codes:

1. **Setup**: First we will have to setup free aws account which can be done [here](https://aws.amazon.com/free/?all-free-tier.sort-by=item.additionalFields.SortRank&all-free-tier.sort-order=asc&awsf.Free%20Tier%20Types=*all&awsf.Free%20Tier%20Categories=*all). Then we need to Download the data from [German Credit Risk](https://www.kaggle.com/uciml/german-credit). We have included it in the data folder in the repository

2. **Cloning the Repository to Cloud SDK**: We will have to copy the repository on Cloud SDK using below command:

```bash
    # clone this repo:
    git clone https://github.com/adityasolanki205/Batch-ETL-processing-using-AWS-Glue.git
```

3. **Creating a Glue job**: Now we create a Glue job to run Pyspark code. 

    - Goto Glue from AWS Console
    
    - Open ETL Jobs from the list
    
    - Select Spark script editor


https://user-images.githubusercontent.com/56908240/230775841-60e29f5e-411f-49a5-88e4-c03c3e9644e8.mp4



4. **Reading data from S3 bucket**: To read the data we will use pyspark code. Here we will use SparkSession to create a dataframe by reading from a input bucket.

```python
    import pyspark
    from pyspark.sql import SparkSession

    appName = "DataProc testing"
    master = "local"
    spark = SparkSession.builder.\
            appName(appName).\
            master(master).\
            getOrCreate()     

    bucket = "dataproc-testing-pyspark"
    spark.conf.set('temporaryGcsBucket', bucket)
    df = spark.read.option( "inferSchema" , "true" ).option("header","true").csv("gs://inputgluetesting/german_data.csv")

``` 

5. **Filtering out unwanted data using Filter()**: Here we will filter out data with Null values

```python
    import findspark
    import pyspark
    from pyspark.sql import SparkSession
    import pyspark.sql.functions as fn
    from pyspark.sql import functions as sf

    #Initializing spark Session builder
    spark = SparkSession.builder\
            .master("local")\
            .appName("Colab")\
            .config('spark.ui.port', '4050')\
            .getOrCreate()
    ...
    
    df = df.filter((df.Purpose != 'NULL') 
                   & (df.Existing_account != 'NULL') 
                   & (df.Property !=  'NULL') 
                   & (df.Personal_status != 'NULL') 
                   & (df.Existing_account != 'NULL')  
                   & (df.Credit_amount != 'NULL' ) 
                   & (df.Installment_plans != 'NULL'))

``` 

6. **Changeing Datatype of certain columns**: Here we will change the datatype of a complete column data using withcolumn().

```python
    import findspark
    import pyspark
    from pyspark.sql import SparkSession
    import pyspark.sql.functions as fn
    from pyspark.sql import functions as sf

    #Initializing spark Session builder
    spark = SparkSession.builder\
            .master("local")\
            .appName("Colab")\
            .config('spark.ui.port', '4050')\
            .getOrCreate()
    ...
    df = df.withColumn("Credit_amount", df['Credit_amount'].cast('float'))

``` 

7. **Converting Encrpyted data to a more readable form**: Here we will decrypt data that us not human readable.

```python
    import findspark
    import pyspark
    from pyspark.sql import SparkSession
    import pyspark.sql.functions as fn
    from pyspark.sql import functions as sf

    #Initializing spark Session builder
    spark = SparkSession.builder\
            .master("local")\
            .appName("Colab")\
            .config('spark.ui.port', '4050')\
            .getOrCreate()
    ...
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

``` 

8. **Dropping redundant Columns**: Here we will remove columns which have been decrypted or are of no use.

```python
    import findspark
    import pyspark
    from pyspark.sql import SparkSession
    import pyspark.sql.functions as fn
    from pyspark.sql import functions as sf

    #Initializing spark Session builder
    spark = SparkSession.builder\
            .master("local")\
            .appName("Colab")\
            .config('spark.ui.port', '4050')\
            .getOrCreate()
    ...
    df = df.drop('day1')
    df = df.drop('day2')
    df = df.drop('ver1')
    df = df.drop('ver2')
    df = df.drop('Purpose')
    df = df.drop('Existing_account')
``` 
10. **Saving the data in another S3 bucket**: At last we will save the data in the S3 Bucket in csv format using the below command

```python
    import findspark
    import pyspark
    from pyspark.sql import SparkSession
    import pyspark.sql.functions as fn
    from pyspark.sql import functions as sf

    #Initializing spark Session builder
    spark = SparkSession.builder\
            .master("local")\
            .appName("Colab")\
            .config('spark.ui.port', '4050')\
            .getOrCreate()
    ...
    df.write.format("csv").mode("append").option('header', 'true').save("s3://outputgluetesting/")

``` 

The output will be available inside the buckets mentioned in the output


## Tests
To test the code we need to do the following:

    1. Copy the repository in Cloud SDK using below command:
        git clone https://github.com/adityasolanki205/Batch-ETL-processing-using-AWS-Glue.git
    
    2. Create a S3 Storage Bucket by the name inputgluetesting in mumbai region.
    
    3. Copy the data file in the cloud Bucket using the below command. To do that open the AWS CLI and use the commands below.
        cd Batch-Processing-using-Dataproc/data
        aws s3 cp german_data.csv s3://inputgluetesting/german_data.csv
        cd ..
        
    4. Create a S3 Storage Bucket by the name outputgluetesting in mumbai region. We will save output data here. 

    5. Create IAM role to give Glue all the access required to perform the tasks. 
        
        - To create a role, goto IAM from the console
        
        - Click in create role
        
        - In use case, search for Glue and click next
        
        - In Permission Policy, select AmazonS3FullAccess, AWSGlueServiceRole  , AWSGlueConsoleFullAccess and click next
        
        - Give this role a name and click on create role. 
        

https://user-images.githubusercontent.com/56908240/230778342-a3b64947-e940-42b4-bb2d-3bb5be18ca4a.mp4


        
    6. Create a database using Glue Data Catalog. This database will be used to fetch data from our Transformed dataset.   
       
        - To create a database, goto Glue Data Catalog and click on Create Database. Verify the region selected is Mumbai on the console.
        
        - Provide any name for the database and click in Create Database.
    
    7. Create a Glue Job to run the Python script to perform the transformations
        
        - Goto AWS Glue from console and click on ETL jobs
        
        - Select Spark script editor
        
        - Now boilerplate script will open. Copy the code present in batch.py from this repository and paste it on the script tab
        
        - Click on save.
        
        - It will throw error to change the names or Python job and Job id for glue. Make the changes and click in save and run
        
        - Verify the job to check for any issues. If there are no issues, job will run successfully
    
    8. 


## Credits
1. Akash Nimare's [README.md](https://gist.github.com/akashnimare/7b065c12d9750578de8e705fb4771d2f#file-readme-md)
2. [Apache Spark](https://spark.apache.org/)
