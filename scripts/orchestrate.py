import boto3
import time

def wait_for_crawler(glue_client, crawler_name):
      while True:
          response = glue_client.get_crawler(Name=crawler_name)
          state = response['Crawler']['State']
          if state == 'READY':
              break
          time.sleep(10)  

def main():
      glue_client = boto3.client('glue')
      redshift_data_client = boto3.client('redshift-data')
      # UAT: Run crawler and create schemas
      glue_client.start_crawler(Name='uat-raw-crawler')
      wait_for_crawler(glue_client, 'uat-raw-crawler')
      redshift_data_client.execute_statement(
          WorkgroupName='uat-workgroup',
          Database='dev',
          Sql='CREATE SCHEMA IF NOT EXISTS bronze; DROP TABLE IF EXISTS bronze.sales; CREATE TABLE bronze.sales (region VARCHAR(255), country VARCHAR(255), item_type VARCHAR(255), sales_channel VARCHAR(255), order_priority VARCHAR(255), order_date VARCHAR(255), order_id BIGINT, ship_date VARCHAR(255), units_sold BIGINT, unit_price DOUBLE PRECISION, unit_cost DOUBLE PRECISION, total_revenue DOUBLE PRECISION, total_cost DOUBLE PRECISION, total_profit DOUBLE PRECISION);'
      )
      redshift_data_client.execute_statement(
          WorkgroupName='uat-workgroup',
          Database='dev',
          Sql='CREATE SCHEMA IF NOT EXISTS gold; DROP TABLE IF EXISTS gold.transformed_data; CREATE TABLE gold.transformed_data (region VARCHAR(255), country VARCHAR(255), item_type VARCHAR(255), sales_channel VARCHAR(255), order_priority VARCHAR(255), order_date VARCHAR(255), order_id BIGINT, ship_date VARCHAR(255), units_sold BIGINT, unit_price DOUBLE PRECISION, unit_cost DOUBLE PRECISION, total_revenue DOUBLE PRECISION, total_cost DOUBLE PRECISION, total_profit DOUBLE PRECISION, profit_margin DOUBLE PRECISION);'
      )
      # PROD: Run crawler and create schemas
      glue_client.start_crawler(Name='prod-raw-crawler')
      wait_for_crawler(glue_client, 'prod-raw-crawler')
      redshift_data_client.execute_statement(
          WorkgroupName='prod-workgroup',
          Database='dev',
          Sql='CREATE SCHEMA IF NOT EXISTS bronze; DROP TABLE IF EXISTS bronze.sales; CREATE TABLE bronze.sales (region VARCHAR(255), country VARCHAR(255), item_type VARCHAR(255), sales_channel VARCHAR(255), order_priority VARCHAR(255), order_date VARCHAR(255), order_id BIGINT, ship_date VARCHAR(255), units_sold BIGINT, unit_price DOUBLE PRECISION, unit_cost DOUBLE PRECISION, total_revenue DOUBLE PRECISION, total_cost DOUBLE PRECISION, total_profit DOUBLE PRECISION);'
      )
      redshift_data_client.execute_statement(
          WorkgroupName='prod-workgroup',
          Database='dev',
          Sql='CREATE SCHEMA IF NOT EXISTS gold; DROP TABLE IF EXISTS gold.transformed_data; CREATE TABLE gold.transformed_data (region VARCHAR(255), country VARCHAR(255), item_type VARCHAR(255), sales_channel VARCHAR(255), order_priority VARCHAR(255), order_date VARCHAR(255), order_id BIGINT, ship_date VARCHAR(255), units_sold BIGINT, unit_price DOUBLE PRECISION, unit_cost DOUBLE PRECISION, total_revenue DOUBLE PRECISION, total_cost DOUBLE PRECISION, total_profit DOUBLE PRECISION, profit_margin DOUBLE PRECISION);'
      )

if __name__ == '__main__':
      main()
  
