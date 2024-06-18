-- Databricks notebook source
-- MAGIC %md
-- MAGIC # How to create tables

-- COMMAND ----------

-- DBTITLE 1,create external table from csv

CREATE EXTERNAL TABLE  IF NOT EXISTS costusage.{environment}_usage.aws_assets
USING csv
OPTIONS (
  'header' 'true',
  'path' 's3://pm-epsilon-athena/databricks/dim_tables/aws_costusage/aws_assets_2024.csv',
  'mergeSchema' 'true'
);

-- COMMAND ----------

-- DBTITLE 1,refresh external table
REFRESH TABLE costusage.{environment}_usage.aws_assets;


-- COMMAND ----------

-- DBTITLE 1,create detla table from select query
DROP TABLE IF EXISTS costusage.dev_usage.fact_aws_assets;
CREATE OR REPLACE TABLE costusage.dev_usage.fact_aws_assets
USING DELTA
LOCATION 's3://pm-epsilon-athena/databricks/dev/dev_usage/fact_aws_assets/'
AS
select
  line_item_usage_account_id,
  hours_of_month,
  tag_costgroup2,
  case
    when tag_name is null
    and resource_id not like 'i-%' then resource_id
    else tag_name
  end tag_name,
  resource_id,
  tag_sp,
  product_servicecode,
  product_product_family,
  product_operating_system,
  product_region_code,
  line_item_usage_type,
  partial_1yr_csp_rate,
  noupfront_1yr_csp_rate,
  pricing_public_on_demand_rate,
  sum(usage_amount) usage_amount,
  sum(pricing_public_on_demand_rate * usage_amount) ondemand_cost
from
  costusage.dev_usage.aws_assets
group by
  line_item_usage_account_id,resource_id,
  hours_of_month,
  tag_costgroup2,  
  case
    when tag_name is null
    and resource_id not like 'i-%' then resource_id
    else tag_name
  end,
  tag_sp,
  product_servicecode,
  product_product_family,
  product_operating_system,
  product_region_code,
  line_item_usage_type,
  partial_1yr_csp_rate,
  noupfront_1yr_csp_rate,
  pricing_public_on_demand_rate

-- COMMAND ----------

-- DBTITLE 1,ways to update tables

update costusage.dev_usage.fact_aws_assets set tag_costgroup2='TITANGERMANY'  where line_item_usage_account_id IN 
('252701498779') and tag_costgroup2 is  null;

MERGE INTO costusage.dev_usage.fact_aws_assets AS aws
USING costusage.source_usage.dim_customtags AS dim
   ON aws.line_item_usage_account_id = dim.name
   AND dim.original = 'SA' 
   and dim.type ='owner_id'
WHEN MATCHED THEN 
   UPDATE SET aws.tag_costgroup2 = upper(ltrim(rtrim(dim.new)));


-- COMMAND ----------

-- DBTITLE 1,window function and last_value
select
accountname,ownerid,hours_of_month,tag_costgroup2, tag_name,tag_sp,product_servicecode,
  product_product_family,
  product_operating_system,
  product_region_code,
  line_item_usage_type,
  partial_1yr_csp_rate,
  noupfront_1yr_csp_rate,
  pricing_public_on_demand_rate,
  sum(usage_amount) as usage_amount,
  case when round(sum(usage_amount)/hours_of_month,0) = 1.0 then '100%' else round(sum(usage_amount)/hours_of_month,0) end as per_usage
from (
select d.accountname,d.ownerid,hours_of_month,tag_costgroup2, case when tag_name is null then line_item_usage_type else tag_name end as tag_name,tag_sp,product_servicecode,
  product_product_family,
  product_operating_system,
  product_region_code,
  line_item_usage_type,
  partial_1yr_csp_rate,
  noupfront_1yr_csp_rate,
  pricing_public_on_demand_rate,
  usage_amount
   from 
(select 
line_item_usage_account_id,
  hours_of_month,
  last_value(tag_costgroup2) over w as tag_costgroup2,
  tag_costgroup2 as o_tag_costgroup2,  
  tag_name,
  last_value(tag_sp) over w as tag_sp,
  product_servicecode,
  product_product_family,
  product_operating_system,
  product_region_code,
  line_item_usage_type,
  partial_1yr_csp_rate,
  noupfront_1yr_csp_rate,
  pricing_public_on_demand_rate,
  usage_amount  
from
costusage.dev_usage.fact_aws_assets assets
WINDOW w as (partition by line_item_usage_account_id,tag_name order by tag_name desc)) a
left join costusage.source_usage.dim_accounts d
on a.line_item_usage_account_id=d.ownerid) assets
group by accountname,ownerid,hours_of_month,tag_costgroup2, tag_name,tag_sp,product_servicecode,
  product_product_family,
  product_operating_system,
  product_region_code,
  line_item_usage_type,
  partial_1yr_csp_rate,
  noupfront_1yr_csp_rate,
  pricing_public_on_demand_rate
having assets.partial_1yr_csp_rate is null

-- COMMAND ----------

-- DBTITLE 1,another example for window function
select 
resource_id
, first_value(tag_costgroup2) IGNORE NULLS over w1 as tag_costgroup2 
, first_value(tag_name) IGNORE NULLS over w1 as tag_name 
,line_item_usage_start_date
from costusage.dev_usage.test_tagallocate
where resource_id is not null
WINDOW w1 as (partition by resource_id order by line_item_usage_start_date desc) 
union all 
select 
*
from costusage.dev_usage.test_tagallocate
where resource_id is null

-- COMMAND ----------

-- DBTITLE 1,python - data ingest with api url
-- MAGIC %python
-- MAGIC # https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/index.json
-- MAGIC # https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonRDS/current/region_index.json
-- MAGIC # https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonRDS/current/us-east-1/index.csv
-- MAGIC
-- MAGIC
-- MAGIC import pandas as pd
-- MAGIC # from  pyspark library import 
-- MAGIC # SparkSession
-- MAGIC from pyspark.sql import SparkSession
-- MAGIC import numpy as np
-- MAGIC # Building the SparkSession and name
-- MAGIC # it :'pandas to spark'
-- MAGIC spark = SparkSession.builder.appName(
-- MAGIC   "pandas to spark").getOrCreate()
-- MAGIC
-- MAGIC # # URL of the CSV file (example URL, replace with actual URL)
-- MAGIC # url = 'https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonRDS/current/us-east-1/index.csv'
-- MAGIC
-- MAGIC # # Load the CSV file into a DataFrame, skipping the first 5 rows
-- MAGIC # df = pd.read_csv(url, skiprows=5, header=0)
-- MAGIC # df = df.fillna(value=np.nan)
-- MAGIC # df.columns = df.columns.str.replace(' ', '_')
-- MAGIC # # # Display the DataFrame
-- MAGIC # # df.head()
-- MAGIC
-- MAGIC # # create DataFrame
-- MAGIC # df_spark = spark.createDataFrame(df)
-- MAGIC  
-- MAGIC # # df_spark.show()
-- MAGIC
-- MAGIC # df_spark.write.format("delta").mode("overwrite").saveAsTable("costusage.aws_usage.aws_skupricing")
-- MAGIC
-- MAGIC # URL of the CSV file (example URL, replace with actual URL)
-- MAGIC url = 'https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonRDS/current/us-east-2/index.csv'
-- MAGIC
-- MAGIC # Load the CSV file into a DataFrame, skipping the first 5 rows
-- MAGIC df = pd.read_csv(url, skiprows=5, header=0)
-- MAGIC df = df.fillna(value=np.nan)
-- MAGIC df.columns = df.columns.str.replace(' ', '_')
-- MAGIC # # Display the DataFrame
-- MAGIC # df.head()
-- MAGIC
-- MAGIC # create DataFrame
-- MAGIC df_spark = spark.createDataFrame(df)
-- MAGIC # Append the DataFrame to the existing table
-- MAGIC df_spark.write.mode("append").saveAsTable("costusage.aws_usage.aws_skupricing")

-- COMMAND ----------

-- DBTITLE 1,create view
create or replace view costusage.aws_usage.reference_rds_pricing
as
select distinct product_servicecode,product_region_code, line_item_usage_type,pricing_public_on_demand_rate,
aws.sku,
aws.usagetype,
aws.Product_Family,
aws.Instance_Type_Family,
aws.Instance_Type,
aws.Current_Generation,
aws.Database_Engine, 
aws.Database_Edition, 
aws.License_Model,
aws.Deployment_Option,
re.purchaseoption,
aws.PricePerUnit as ondemand_rate,
case when re.PurchaseOption = 'All Upfront' then re.PricePerUnit/12/730 else re.PricePerUnit end as PricePerUnit,
'Hrs' as Unit,
re.RateCode
from costusage.dev_usage.fact_aws_assets as dim
left join costusage.aws_usage.aws_skupricing aws on
dim.line_item_usage_type =aws.usageType 
and dim.product_region_code = aws.Region_Code
and dim.pricing_public_on_demand_rate = aws.PricePerUnit
left join costusage.aws_usage.aws_skupricing re
on aws.sku = re.sku
and aws.usageType =re.usageType 
and aws.Region_Code = re.Region_Code
-- and aws.Database_Engine = re.Database_Engine
-- -- and aws.Database_Edition =  re.Database_Edition
-- and aws.License_Model = re.License_Model
-- and aws.Deployment_Option = re.Deployment_Option
where aws.TermType ='OnDemand'
and re.TermType ='Reserved' 
and((re.Unit ='Hrs' and re.PurchaseOption != 'All Upfront') or 
(re.Unit ='Quantity' and re.PurchaseOption = 'All Upfront'))  
and (re.LeaseContractLength ='1yr') 
and dim.product_servicecode ='AmazonRDS'
-- and (aws.Database_Edition !='Standard Two' or aws.Database_Edition is null)
-- and aws.region_Code ='eu-west-1'


-- COMMAND ----------

-- MAGIC %md
-- MAGIC # using parameters

-- COMMAND ----------

-- DBTITLE 1,database ${environment}_usage
CREATE DATABASE IF NOT EXISTS costusage.{environment}_usage
MANAGED LOCATION 's3://pm-epsilon-athena/databricks/{environment}/';

-- COMMAND ----------

-- DBTITLE 1,python: create table with excel
-- MAGIC %python
-- MAGIC import pandas as pd
-- MAGIC import boto3
-- MAGIC from io import BytesIO
-- MAGIC from datetime import datetime
-- MAGIC
-- MAGIC aws_access_key_id = dbutils.widgets.get("aws_access_key_id")
-- MAGIC
-- MAGIC aws_secret_access_key = dbutils.widgets.get("aws_secret_access_key")
-- MAGIC
-- MAGIC aws_region = dbutils.widgets.get("aws_region")
-- MAGIC
-- MAGIC bucket_name = 'pm-epsilon-athena'
-- MAGIC file_key = 'awscostreport/2024/All_Projects_Breakdown_2024.xlsx'
-- MAGIC
-- MAGIC s3_client = boto3.client(
-- MAGIC     's3',
-- MAGIC     aws_access_key_id=aws_access_key_id,
-- MAGIC     aws_secret_access_key=aws_secret_access_key,
-- MAGIC     region_name=aws_region
-- MAGIC )
-- MAGIC
-- MAGIC # Read the Excel file from S3
-- MAGIC response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
-- MAGIC excel_data = response['Body'].read()
-- MAGIC
-- MAGIC # Load the Excel data into a Pandas DataFrame
-- MAGIC df = pd.read_excel(BytesIO(excel_data), sheet_name='Summary_CrossCharge')
-- MAGIC df['Date'] = df['Date'].dt.date
-- MAGIC display(df['Date'])
-- MAGIC # Display the DataFrame
-- MAGIC
-- MAGIC
-- MAGIC # # Get the first day of the current month
-- MAGIC # first_day_of_month = datetime.now().replace(day=1)
-- MAGIC # print(first_day_of_month)
-- MAGIC # # Filter the DataFrame to include only rows before the first day of the current month
-- MAGIC # filtered_df = df[df['Date'] <  pd.to_datetime(first_day_of_month)]
-- MAGIC # filtered_df['Date'] = df['Date'].dt.date
-- MAGIC # display(filtered_df.head())
-- MAGIC
-- MAGIC # # # Now you can work with the filtered DataFrame (filtered_df)
-- MAGIC
-- MAGIC # # # Load filtered_df into a table
-- MAGIC # # # Assuming you have a table named 'my_table' in your Databricks environment
-- MAGIC # # # You can use Spark DataFrame's toPandas() method to convert Pandas DataFrame to Spark DataFrame
-- MAGIC
-- MAGIC # from pyspark.sql import SparkSession
-- MAGIC
-- MAGIC # spark = SparkSession.builder \
-- MAGIC #     .appName("example-app") \
-- MAGIC #     .getOrCreate()
-- MAGIC
-- MAGIC # spark_df = spark.createDataFrame(filtered_df)
-- MAGIC # spark_df.printSchema()
-- MAGIC # spark_df.write.format("delta").mode("overwrite").saveAsTable("costusage.source_usage.aws_costusage_2024")
-- MAGIC

-- COMMAND ----------

-- DBTITLE 1,source_awscur
DROP TABLE costusage.source_usage.source_awscur;
CREATE TABLE IF NOT EXISTS costusage.source_usage.source_awscur
USING parquet
OPTIONS (
  'path' 's3://pm-epsilon-cur-athena/pm-cur-athena/pm_epsilon_cur_athena/pm_epsilon_cur_athena',
  'mergeSchema' 'true'
);

-- COMMAND ----------

-- DBTITLE 1,process date
select
  date_format(my_date, 'yyyy-MM') as yearmonth,
  YEAR(my_date) AS year,
  MONTH(my_date) AS month,
  DAY(last_day(my_date)) * 24 AS total_hours_in_month
FROM
  (SELECT CAST('2023-01-01' AS DATE) AS my_date) AS t;

