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

# Script generated for node step_trainer_landing
step_trainer_landing_node1723402976346 = glueContext.create_dynamic_frame.from_catalog(database="stedidb", table_name="step_trainer_landing", transformation_ctx="step_trainer_landing_node1723402976346")

# Script generated for node customers_curated
customers_curated_node1723402895859 = glueContext.create_dynamic_frame.from_catalog(database="stedidb", table_name="customers_curated", transformation_ctx="customers_curated_node1723402895859")

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1723412099883 = ApplyMapping.apply(frame=customers_curated_node1723402895859, mappings=[("serialnumber", "string", "right_serialnumber", "string"), ("sharewithresearchasofdate", "long", "right_sharewithresearchasofdate", "long"), ("sharewithfriendsasofdate", "long", "right_sharewithfriendsasofdate", "long"), ("registrationdate", "long", "right_registrationdate", "long"), ("sharewithpublicasofdate", "long", "right_sharewithpublicasofdate", "long"), ("lastupdatedate", "long", "right_lastupdatedate", "long")], transformation_ctx="RenamedkeysforJoin_node1723412099883")

# Script generated for node SQL Query
SqlQuery7660 = '''
select distinct sensorreadingtime, 
serialnumber, 
distancefromobject from 
step inner join cur
on step.serialnumber = cur.right_serialnumber;
'''
SQLQuery_node1723413141111 = sparkSqlQuery(glueContext, query = SqlQuery7660, mapping = {"cur":RenamedkeysforJoin_node1723412099883, "step":step_trainer_landing_node1723402976346}, transformation_ctx = "SQLQuery_node1723413141111")

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1723403518162 = glueContext.getSink(path="s3://iam-stedi-bucket/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="step_trainer_trusted_node1723403518162")
step_trainer_trusted_node1723403518162.setCatalogInfo(catalogDatabase="stedidb",catalogTableName="step_trainer_trusted")
step_trainer_trusted_node1723403518162.setFormat("json")
step_trainer_trusted_node1723403518162.writeFrame(SQLQuery_node1723413141111)
job.commit()