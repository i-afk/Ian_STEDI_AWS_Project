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

# Script generated for node customer_landing
customer_landing_node1723318335963 = glueContext.create_dynamic_frame.from_catalog(database="stedidb", table_name="customer_landing", transformation_ctx="customer_landing_node1723318335963")

# Script generated for node accelerometer_landing
accelerometer_landing_node1723318981998 = glueContext.create_dynamic_frame.from_catalog(database="stedidb", table_name="accelerometer_landing", transformation_ctx="accelerometer_landing_node1723318981998")

# Script generated for node Join
Join_node1723319116964 = Join.apply(frame1=customer_landing_node1723318335963, frame2=accelerometer_landing_node1723318981998, keys1=["email"], keys2=["user"], transformation_ctx="Join_node1723319116964")

# Script generated for node sanitize
SqlQuery7556 = '''
select user, timestamp, x, y, z from myDataSource where sharewithresearchasofdate is not null
'''
sanitize_node1723318357079 = sparkSqlQuery(glueContext, query = SqlQuery7556, mapping = {"myDataSource":Join_node1723319116964}, transformation_ctx = "sanitize_node1723318357079")

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1723318430435 = glueContext.getSink(path="s3://iam-stedi-bucket/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="accelerometer_trusted_node1723318430435")
accelerometer_trusted_node1723318430435.setCatalogInfo(catalogDatabase="stedidb",catalogTableName="accelerometer_trusted")
accelerometer_trusted_node1723318430435.setFormat("json")
accelerometer_trusted_node1723318430435.writeFrame(sanitize_node1723318357079)
job.commit()