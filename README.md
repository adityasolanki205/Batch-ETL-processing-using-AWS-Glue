# ETL batch Processing using AWS Glue
This is the part of **ETL processing AWS** Repository. Here we will try to learn basics of Apache Spark to create **Batch** jobs on AWS Glue. In this repo We will learn step by step process to create a batch job using [German Credit Risk](https://www.kaggle.com/uciml/german-credit). The complete process is divided into 6 parts:

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
    
    2. Create a US Multiregional Storage Bucket by the name dataproc-testing-pyspark.
    
    3. Copy the data file in the cloud Bucket using the below command
        cd Batch-Processing-using-Dataproc/data
        gsutil cp german_data.csv gs://dataproc-testing-pyspark/
        cd ..

    4. Create Temporary variables to hold GCP values
        PROJECT=<project name>
        BUCKET_NAME=dataproc-testing-pyspark
        CLUSTER=testing-dataproc
        REGION=us-central1
        
    5. Create a Biquery dataset with the name GermanCredit and a table named German_Credit_final. 
       This should be an empty table with schema as given below:
       
        Duration_month:INTEGER,
        Credit_history:STRING,
        Credit_amount:FLOAT,
        Saving:STRING,
        Employment_duration:STRING,
        Installment_rate:INTEGER,
        Personal_status:STRING,
        Debtors:STRING,
        Residential_Duration:INTEGER,
        Property:STRING,
        Age:INTEGER,
        Installment_plans:STRING,
        Housing:STRING,
        Number_of_credits:INTEGER,
        Job:STRING,
        Liable_People:INTEGER,
        Telephone:STRING,
        Foreign_worker:STRING,
        Classification:INTEGER,
        Month:STRING,
        Days:STRING,
        File_month:STRING,
        Version:STRING
    
    6. Create a Dataproc cluster by using the command:
        gcloud dataproc clusters create ${CLUSTER} \
        --project=${PROJECT} \
        --region=${REGION} \
        --single-node 
    
    7. Create a PySpark Job to run the code:
        gcloud dataproc jobs submit pyspark Batch.py \
        --cluster=${CLUSTER} \
        --region=${REGION} \
        --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar


## Credits
1. Akash Nimare's [README.md](https://gist.github.com/akashnimare/7b065c12d9750578de8e705fb4771d2f#file-readme-md)
2. [Apache Spark](https://spark.apache.org/)
