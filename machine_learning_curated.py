import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node accelerometer-trusted
accelerometertrusted_node1723416034981 = glueContext.create_dynamic_frame.from_catalog(database="stedidb", table_name="accelerometer_trusted", transformation_ctx="accelerometertrusted_node1723416034981")

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1723416033591 = glueContext.create_dynamic_frame.from_catalog(database="stedidb", table_name="step_trainer_trusted", transformation_ctx="step_trainer_trusted_node1723416033591")

# Script generated for node Join
Join_node1723416168767 = Join.apply(frame1=accelerometertrusted_node1723416034981, frame2=step_trainer_trusted_node1723416033591, keys1=["timestamp"], keys2=["sensorreadingtime"], transformation_ctx="Join_node1723416168767")

# Script generated for node SQL Query
SqlQuery7178 = '''
select 	
serialnumber,
timestamp,
distancefromobject,
x, y, z
from myDataSource;
'''
SQLQuery_node1723416300077 = sparkSqlQuery(glueContext, query = SqlQuery7178, mapping = {"myDataSource":Join_node1723416168767}, transformation_ctx = "SQLQuery_node1723416300077")

# Script generated for node Amazon S3
AmazonS3_node1723416401135 = glueContext.getSink(path="s3://iam-stedi-bucket/machine_learning_curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1723416401135")
AmazonS3_node1723416401135.setCatalogInfo(catalogDatabase="stedidb",catalogTableName="machine_learning_curated")
AmazonS3_node1723416401135.setFormat("json")
AmazonS3_node1723416401135.writeFrame(SQLQuery_node1723416300077)
job.commit()