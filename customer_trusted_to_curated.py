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

# Script generated for node customer_trusted
customer_trusted_node1723318335963 = glueContext.create_dynamic_frame.from_catalog(database="stedidb", table_name="customer_trusted", transformation_ctx="customer_trusted_node1723318335963")

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1723318981998 = glueContext.create_dynamic_frame.from_catalog(database="stedidb", table_name="accelerometer_trusted", transformation_ctx="accelerometer_trusted_node1723318981998")

# Script generated for node cust + acc join
custaccjoin_node1723319116964 = Join.apply(frame1=customer_trusted_node1723318335963, frame2=accelerometer_trusted_node1723318981998, keys1=["email"], keys2=["user"], transformation_ctx="custaccjoin_node1723319116964")

# Script generated for node sanitize
SqlQuery7914 = '''
select distinct serialnumber, shareWithResearchAsOfDate, shareWithFriendsAsOfDate, registrationDate, shareWithPublicAsOfDate, lastUpdateDate from myDataSource where sharewithresearchasofdate is not null;
'''
sanitize_node1723318357079 = sparkSqlQuery(glueContext, query = SqlQuery7914, mapping = {"myDataSource":custaccjoin_node1723319116964}, transformation_ctx = "sanitize_node1723318357079")

# Script generated for node customers_curated
customers_curated_node1723327049116 = glueContext.write_dynamic_frame.from_options(frame=sanitize_node1723318357079, connection_type="s3", format="json", connection_options={"path": "s3://iam-stedi-bucket/customer/curated/", "partitionKeys": []}, transformation_ctx="customers_curated_node1723327049116")

job.commit()