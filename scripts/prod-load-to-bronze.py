import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Read from S3 using Glue Catalog 
source_data = glueContext.create_dynamic_frame.from_catalog(
    database="prod_glue_catalog",
    table_name="raw", 
    transformation_ctx="source_data"
)

# Write to Redshift Bronze table (using the connection)
glueContext.write_dynamic_frame.from_jdbc_conf(
    frame=source_data,
    catalog_connection="prod-redshift-connection",
    connection_options={
        "dbtable": "bronze.sales",  # Schema.table
        "database": "dev"
    },
    redshift_tmp_dir="s3://prod-bucket-tl/temp/",  # Temp folder for staging data
    transformation_ctx="write_to_redshift"
)

job.commit()
