from pyspark.sql import SparkSession

if __name__ == "__main__":

    # build spark session
    spark = SparkSession.builder.appName("KoalasPostgresDemo").getOrCreate()

    # Enable hadoop s3a settings
    spark._jsc.hadoopConfiguration().set("com.amazonaws.services.s3.enableV4", "true")
    spark._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider", \
    "com.amazonaws.auth.InstanceProfileCredentialsProvider,com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
    spark._jsc.hadoopConfiguration().set("fs.AbstractFileSystem.s3a.impl", "org.apache.hadoop.fs.s3a.S3A")

    # read data from publc bucket into Spark DF
    data_path = "s3a://warehouse3/employee/" 
    df = spark.read.parquet(data_path)

    # write to db
    finalDF.write.parquet('s3a://warehouse3/employee_1/')