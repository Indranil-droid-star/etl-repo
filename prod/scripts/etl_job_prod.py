import sys
import boto3
import time
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import lit, row_number, when
from pyspark.sql.window import Window

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Step 1: Run crawler
glue_client = boto3.client('glue')
crawler_name = 'prod-raw-crawler'
glue_client.start_crawler(Name=crawler_name)
while True:
    response = glue_client.get_crawler(Name=crawler_name)
    if response['Crawler']['State'] == 'READY':
        break
    time.sleep(10)

# Step 2: Combine data from catalog tables
catalog_tables = ['prod_sales_csv', 'prod_sales_parquet']
table1st = ''
dynamic_frames = []
for table_name in catalog_tables:
    try:
        df = glueContext.create_dynamic_frame.from_catalog(
            database="prod_glue_catalog",
            table_name=table_name,
            transformation_ctx=f"source_{table_name}"
        )
        dynamic_frames.append(df)
        if table1st == '':
            table1st = table_name
    except Exception as e:
        print(f"Error reading table {table_name}: {str(e)}")
        continue

if dynamic_frames:
    spark_dfs = [df.toDF() for df in dynamic_frames]
    common_columns = set(spark_dfs[0].columns)
    for df in spark_dfs[1:]:
        common_columns = common_columns.intersection(set(df.columns))
    common_columns = list(common_columns)
    unified_df = spark_dfs[0].select(common_columns)
    for df in spark_dfs[1:]:
        unified_df = unified_df.union(df.select(common_columns))
    unified_dynamic_frame = DynamicFrame.fromDF(unified_df, glueContext, "unified_data")
else:
    raise Exception("No valid data found in catalog tables")

# Step 3: Create bronze schema dynamically
redshift_data_client = boto3.client('redshift-data')
workgroup_name = 'prod-workgroup'
database = 'dev'

response = glue_client.get_table(DatabaseName='prod_glue_catalog', Name=table1st)
columns = [col['Name'] for col in response['Table']['StorageDescriptor']['Columns']]
base_schema = ['region', 'country', 'item_type', 'sales_channel', 'order_priority', 
               'order_date', 'order_id', 'ship_date', 'units_sold', 'unit_price', 
               'unit_cost', 'total_revenue', 'total_cost', 'total_profit']
new_columns = [col for col in columns if col not in base_schema]
sql_bronze = f"""
CREATE SCHEMA IF NOT EXISTS bronze;
DROP TABLE IF EXISTS bronze.sales;
CREATE TABLE bronze.sales (
    region VARCHAR(255),
    country VARCHAR(255),
    item_type VARCHAR(255),
    sales_channel VARCHAR(255),
    order_priority VARCHAR(255),
    order_date VARCHAR(255),
    order_id BIGINT,
    ship_date VARCHAR(255),
    units_sold BIGINT,
    unit_price DOUBLE PRECISION,
    unit_cost DOUBLE PRECISION,
    total_revenue DOUBLE PRECISION,
    total_cost DOUBLE PRECISION,
    total_profit DOUBLE PRECISION{''.join([f', {col} VARCHAR(255)' for col in new_columns])}
);
"""
redshift_data_client.execute_statement(
    WorkgroupName=workgroup_name,
    Database=database,
    Sql=sql_bronze
)
time.sleep(10)

# Step 4: Load to bronze.sales
glueContext.write_dynamic_frame.from_jdbc_conf(
    frame=unified_dynamic_frame,
    catalog_connection="prod-redshift-connection",
    connection_options={"dbtable": "bronze.sales", "database": "dev"},
    redshift_tmp_dir="s3://prod-bucket-tl/temp/",
    transformation_ctx="write_to_redshift"
)

# Step 5: Create gold schema dynamically
sql_gold = f"""
CREATE SCHEMA IF NOT EXISTS gold;
DROP TABLE IF EXISTS gold.transformed_data;
CREATE TABLE gold.transformed_data (
    region VARCHAR(255),
    country VARCHAR(255),
    item_type VARCHAR(255),
    sales_channel VARCHAR(255),
    order_priority VARCHAR(255),
    order_date VARCHAR(255),
    order_id BIGINT,
    ship_date VARCHAR(255),
    units_sold BIGINT,
    unit_price DOUBLE PRECISION,
    unit_cost DOUBLE PRECISION,
    total_revenue DOUBLE PRECISION,
    total_cost DOUBLE PRECISION,
    total_profit DOUBLE PRECISION,
    profit_margin DOUBLE PRECISION,
    impact DOUBLE PRECISION{''.join([f', {col} VARCHAR(255)' for col in new_columns])}
);
"""
redshift_data_client.execute_statement(
    WorkgroupName=workgroup_name,
    Database=database,
    Sql=sql_gold
)
time.sleep(10)

# Step 6: Transform to gold.transformed_data
window_spec = Window.partitionBy("order_id").orderBy(unified_df["order_date"].desc())
gold_df = unified_df.withColumn("rn", row_number().over(window_spec)) \
                    .filter(col("rn") == 1) \
                    .drop("rn") \
                    .withColumn("profit_margin", when(col("total_revenue") > 0, (col("total_profit") / col("total_revenue")) * 100).otherwise(0)) \
                    .withColumn("impact", col("total_profit") * 0.05)
gold_dynamic_frame = DynamicFrame.fromDF(gold_df, glueContext, "gold_data")

glueContext.write_dynamic_frame.from_jdbc_conf(
    frame=gold_dynamic_frame,
    catalog_connection="prod-redshift-connection",
    connection_options={"dbtable": "gold.transformed_data", "database": "dev"},
    redshift_tmp_dir="s3://prod-bucket-tl/temp/",
    transformation_ctx="write_to_gold"
)

# Step 7: Export to CSV
gold_df.coalesce(1).write.mode("overwrite").option("header", "true").csv("s3://prod-bucket-tl/output/gold_transformed_data")

job.commit()
