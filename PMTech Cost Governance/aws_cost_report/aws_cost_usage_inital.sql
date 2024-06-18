-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### meta data

-- COMMAND ----------

-- DBTITLE 1,database ${environment}_usage
CREATE DATABASE IF NOT EXISTS costusage.aws_usage
MANAGED LOCATION 's3://pm-epsilon-athena/databricks/aws/';

-- COMMAND ----------

-- DBTITLE 1,aws_costusage_2023
drop table if exists costusage.aws_usage.aws_costusage_2023;
CREATE EXTERNAL TABLE  IF NOT EXISTS costusage.aws_usage.aws_costusage_2023
(
  `date` date,
  `aws_detailed_service` STRING,
  `usage` float,
  `products` STRING,
  `category` STRING,
  `group` STRING,
  `jobcode` STRING
)
USING csv
OPTIONS (
  'header' 'true',
  'path' 's3://pm-epsilon-athena/databricks/dim_tables/aws_costusage/aws_costusage_2023.csv',
  'mergeSchema' 'true'
);

-- COMMAND ----------

REFRESH TABLE costusage.aws_usage.aws_costusage_2023;
select * from costusage.aws_usage.aws_costusage_2023

-- COMMAND ----------

-- DBTITLE 1,aws_costusage_2024_current
drop table costusage.aws_usage.aws_costusage_2024_current;
CREATE EXTERNAL TABLE  IF NOT EXISTS costusage.aws_usage.aws_costusage_2024_current
(
  `date` date,
  `aws_detailed_service` STRING,
  `usage` float,
  `products` STRING,
  `category` STRING,
  `group` STRING,
  `jobcode` STRING
)
USING csv
OPTIONS (
  'header' 'true',
  'path' 's3://pm-epsilon-athena/databricks/dim_tables/aws_costusage/aws_costusage_2024.csv',
  'mergeSchema' 'true'
);

-- COMMAND ----------

REFRESH TABLE costusage.aws_usage.aws_costusage_2024_current;
select * from costusage.aws_usage.aws_costusage_2024_current order by date desc 

-- COMMAND ----------

-- DBTITLE 1,python script to load from excel
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

-- DBTITLE 1,dim_customtags

CREATE EXTERNAL TABLE IF NOT EXISTS costusage.source_usage.dim_customtags
(
  `type` STRING,
  `name` STRING,
  `original` STRING,
  `new` STRING
)
USING csv
OPTIONS (
  'header' 'false',
  'inferSchema' 'true',
  'path' 's3://pm-epsilon-athena/databricks/dim_tables/dim_customtags/dim_customtags.csv',
  'mergeSchema' 'true'
);
REFRESH TABLE costusage.source_usage.dim_customtags; 

-- COMMAND ----------

-- DBTITLE 1,dim_accounts
CREATE EXTERNAL TABLE  IF NOT EXISTS costusage.source_usage.dim_accounts
USING csv
OPTIONS (
  'header' 'true',
  'path' 's3://pm-epsilon-athena/databricks/dim_tables/dim_accounts/dim_accounts.csv',
  'mergeSchema' 'true'
);

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


select *,date_format(date, 'yyyy-MM') as yearmonth from costusage.${environment}_usage.aws_costusage_2024_current

-- COMMAND ----------

select date_format(dateadd(DAY, 1, current_date()), 'yyyy-MM-dd')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### View Ininital

-- COMMAND ----------

-- DBTITLE 1,view  v_awscur_previous
create or replace view costusage.${environment}_usage.v_awscur_previous
as
SELECT
  year
, month
, line_item_usage_start_date usage_time
, bill_billing_entity
, line_item_usage_account_id owner_id
, UPPER(TRIM(resource_tags_user_cost_group1))  tag_costgroup1
, UPPER(TRIM(resource_tags_user_cost_group2))  tag_costgroup2
, UPPER(TRIM(resource_tags_user_name)) tag_name
, UPPER(TRIM(resource_tags_user_s_p)) tag_sp
, resource_tags_user_cluster_id tag_clusterid
, resource_tags_user_cluster_name tag_clustername
, UPPER(TRIM(resource_tags_user_environment)) tag_env
, UPPER(TRIM(resource_tags_user_project)) tag_project
, UPPER(TRIM(resource_tags_user_client)) tag_client
, UPPER(TRIM(resource_tags_user_agency)) tag_agency
, UPPER(TRIM(resource_tags_user_product)) tag_product
, UPPER(TRIM(resource_tags_user_owner)) tag_owner
, UPPER(TRIM(resource_tags_user_scope)) tag_scope
, UPPER(TRIM(resource_tags_user_ambiente)) tag_italyambiente
, UPPER(TRIM(resource_tags_user_cost_group_1)) tag_italycostgroup1
, UPPER(TRIM(resource_tags_user_cost_group_2)) tag_italycostgroup2
, UPPER(TRIM(resource_tags_user_cliente)) tag_italycliente
, (CASE 
WHEN ((bill_billing_entity = 'AWS Marketplace') OR (line_item_product_code like '%Support%')) THEN product_product_name 
WHEN (concat(concat(line_item_resource_id, savings_plan_savings_plan_a_r_n), reservation_reservation_a_r_n) = '') THEN concat('item_id:',identity_line_item_id)
WHEN ((line_item_resource_id = '') AND (reservation_reservation_a_r_n <> '')) THEN concat('ri_arn:',reservation_reservation_a_r_n) 
WHEN ((line_item_resource_id = '') AND (savings_plan_savings_plan_a_r_n <> '')) THEN concat('sp_arn:',savings_plan_savings_plan_a_r_n)
WHEN (line_item_resource_id <> '') THEN line_item_resource_id ELSE line_item_resource_id END) resource_id
, (CASE WHEN (bill_billing_entity = 'AWS Marketplace') THEN bill_billing_entity 
WHEN ((line_item_product_code LIKE '%Savings%') AND (bill_bill_type = 'Purchase')) THEN concat(line_item_product_code, ' Purchase') 
WHEN (line_item_line_item_type = 'Tax') THEN line_item_line_item_type 
WHEN (line_item_product_code like '%Support%') 
THEN product_product_name ELSE line_item_product_code END) aws_service
, product_product_name service_name
, (CASE WHEN ((line_item_product_code LIKE '%Savings%') AND (bill_bill_type = 'Purchase')) THEN 'Purchase' 
ELSE product_product_family END) service_family
, line_item_operation service_operation
, product_region service_region
, product_operating_system service_os
, product_instance_type_family instance_family
, product_instance_type instance_type
, product_marketoption
, product_license_model as license_model
, product_tenancy
, pricing_term
, pricing_unit
, product_purchase_option as purchase_option
, product_database_edition as database_edition
, product_database_engine as database_engine
, product_deployment_option as deployment_option
, line_item_usage_type usage_type
, line_item_line_item_type item_type
, line_item_usage_amount usage_amount
, line_item_net_unblended_cost net_unblended_cost
, pricing_public_on_demand_cost ondemand_cost
, pricing_public_on_demand_rate ondemand_rate
-- , (CASE WHEN (line_item_line_item_type = 'RIFee') THEN line_item_unblended_rate ELSE '0' END) ri_rate
-- , savings_plan_savings_plan_rate sp_rate
-- , reservation_net_effective_cost ri_net_effective_cost
-- , savings_plan_net_savings_plan_effective_cost sp_net_effective_cost
-- , reservation_effective_cost ri_effective_cost
-- , savings_plan_savings_plan_effective_cost sp_effective_cost
, product_sku
FROM
  costusage.source_usage.source_awscur cur
WHERE (1=1)
AND (line_item_usage_start_date >= date_format(dateadd(MONTH, ${previous_month}, current_date()), 'yyyy-MM-01'))
AND (
  line_item_net_unblended_cost <> 0 OR reservation_net_effective_cost <> 0 OR reservation_net_recurring_fee_for_usage <> 0 OR reservation_net_unused_recurring_fee <> 0 OR savings_plan_net_savings_plan_effective_cost <> 0 OR savings_plan_net_savings_plan_effective_cost <> 0 OR (line_item_net_unblended_cost = 0 AND 
    line_item_line_item_type = 'RIFee'))
-- and (month = month(date_format(dateadd(MONTH, -1, current_date()), 'yyyy-MM-01')) and year = year(date_format(dateadd(MONTH, -1, current_date()), 'yyyy-MM-01')))

-- COMMAND ----------

-- DBTITLE 1,view  v_awscur_current
create or replace view costusage.${environment}_usage.v_awscur_current
as
select * from costusage.${environment}_usage.v_awscur_previous
where usage_time >= date_format(dateadd(MONTH, 0, current_date()), 'yyyy-MM-01') 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## RETAG BY ID

-- COMMAND ----------

-- DBTITLE 1,v_awscur_previous_retagbyid
create or replace VIEW costusage.${environment}_usage.v_awscur_previous_retagbyid
as
select year
,month
,usage_time
,bill_billing_entity
, owner_id
, first_value(tag_costgroup1) IGNORE NULLS over w as tag_costgroup1
, first_value(tag_costgroup2) IGNORE NULLS over w as tag_costgroup2 
, first_value(tag_name) IGNORE NULLS over w as tag_name 
, first_value(tag_sp) IGNORE NULLS over w as tag_sp 
, tag_clusterid
, tag_clustername
, first_value(tag_env) IGNORE NULLS over w as tag_env 
, first_value(tag_project) IGNORE NULLS over w as tag_project 
, first_value(tag_client) IGNORE NULLS over w as tag_client  
, first_value(tag_agency) IGNORE NULLS over w as tag_agency   
, first_value(tag_product) IGNORE NULLS over w as tag_product 
, first_value(tag_owner) IGNORE NULLS over w as tag_owner 
, first_value(tag_scope) IGNORE NULLS over w as tag_scope  
, first_value(tag_italyambiente) IGNORE NULLS over w as tag_italyambiente   
, first_value(tag_italycostgroup1) IGNORE NULLS over w as tag_italycostgroup1  
, first_value(tag_italycostgroup2) IGNORE NULLS over w as tag_italycostgroup2   
, first_value(tag_italycliente) IGNORE NULLS over w as tag_italycliente  
, resource_id
, aws_service
, service_name
, service_family
, service_operation
, service_region
, service_os
, instance_family
, instance_type
, product_marketoption
, license_model
, product_tenancy
, pricing_term
, pricing_unit
, purchase_option
, database_edition
, database_engine
, deployment_option
, usage_type
, item_type
, usage_amount
, net_unblended_cost
, ondemand_cost
, ondemand_rate
-- , ri_rate
-- , sp_rate
-- , ri_net_effective_cost
-- , sp_net_effective_cost
-- , ri_effective_cost
-- , sp_effective_cost
, product_sku
, tag_costgroup2 as original_tagcostgroup2
, tag_project as original_tagproject
from costusage.${environment}_usage.v_awscur_previous
-- where resource_id ='pmx-precisionmediaplatform'
where (resource_id is not null and  resource_id <>  "")
WINDOW w as (partition by resource_id order by usage_time desc)
union all 
select *, tag_costgroup2 as original_tagcostgroup2
, tag_project as original_tagproject from costusage.${environment}_usage.v_awscur_previous
where (resource_id is null or resource_id = "")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## RETAG BY NAME

-- COMMAND ----------

-- DBTITLE 1,v_awscur_previous_retagbyIDname
create 
--MATERIALIZED 
or replace
VIEW costusage.${environment}_usage.v_awscur_previous_retagbyIDname
as 
select 
year
,month
,usage_time
,bill_billing_entity
, owner_id
, first_value(tag_costgroup1) IGNORE NULLS over w as tag_costgroup1
, first_value(tag_costgroup2) IGNORE NULLS over w as tag_costgroup2 
, first_value(tag_name) IGNORE NULLS over w as tag_name 
, first_value(tag_sp) over w as tag_sp 
, tag_clusterid
, tag_clustername
, first_value(tag_env) IGNORE NULLS over w as tag_env 
, first_value(tag_project) IGNORE NULLS over w as tag_project 
, first_value(tag_client) IGNORE NULLS over w as tag_client  
, first_value(tag_agency) IGNORE NULLS over w as tag_agency   
, first_value(tag_product) IGNORE NULLS over w as tag_product 
, first_value(tag_owner) IGNORE NULLS over w as tag_owner
, first_value(tag_scope) IGNORE NULLS over w as tag_scope  
, first_value(tag_italyambiente) IGNORE NULLS over w as tag_italyambiente   
, first_value(tag_italycostgroup1) IGNORE NULLS over w as tag_italycostgroup1  
, first_value(tag_italycostgroup2) IGNORE NULLS over w as tag_italycostgroup2   
, first_value(tag_italycliente) IGNORE NULLS over w as tag_italycliente  
, resource_id
, aws_service
, service_name
, service_family
, service_operation
, service_region
, service_os
, instance_family
, instance_type
, product_marketoption
, license_model
, product_tenancy
, pricing_term
, pricing_unit
, purchase_option
, database_edition
, database_engine
, deployment_option
, usage_type
, item_type
, usage_amount
, net_unblended_cost
, ondemand_cost
, ondemand_rate
-- , ri_rate
-- , sp_rate
-- , ri_net_effective_cost
-- , sp_net_effective_cost
-- , ri_effective_cost
-- , sp_effective_cost
, product_sku
, original_tagcostgroup2 
, original_tagproject 
from costusage.${environment}_usage.v_awscur_previous_retagbyid
where (tag_name is not null and tag_name <> "")
WINDOW w as (partition by tag_name order by usage_time desc)
UNION ALL
select *  from costusage.${environment}_usage.v_awscur_previous_retagbyid where (tag_name is null or tag_name = "")


-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Report Views:

-- COMMAND ----------

-- DBTITLE 1,rpt_view custom tag reallocated
create or replace
VIEW costusage.${environment}_usage.v_awscur_previous_reallocated
as 
SELECT
  year,
  month,
  usage_time,
  bill_billing_entity,
  owner_id,
  tag_costgroup1,
  CASE 
    WHEN (sa.original = 'SA') THEN sa.new 
    WHEN (sa.original = 'SCOPE') THEN 
      (CASE WHEN (upper(tag_scope) = 'EMEADATALAKE') THEN 'DATALAKE EMEA' ELSE sa.new END) 
    WHEN (sa.original = 'COSTGROUP2') 
      THEN COALESCE(tg.new, COALESCE(NULLIF(tag_costgroup2, ''), COALESCE(sa.new, ''))) 
      ELSE COALESCE(tg.new, COALESCE(NULLIF(tag_costgroup2, ''), COALESCE(re.new, ''))) END as tag_costgroup2,
  tag_name,
  tag_sp,
  tag_clusterid,
  tag_clustername,
  tag_env,
  tag_project,
  tag_client,
  tag_agency,
  tag_product,
  tag_owner,
  tag_scope,
  tag_italyambiente,
  tag_italycostgroup1,
  tag_italycostgroup2,
  tag_italycliente
, resource_id
, aws_service
, service_name
, service_family
, service_operation
, service_region
, service_os
, instance_family
, instance_type
, product_marketoption
, license_model
, product_tenancy
, pricing_term
, pricing_unit
, purchase_option
, database_edition
, database_engine
, deployment_option
, usage_type
, item_type
, usage_amount
, net_unblended_cost
, ondemand_cost
, ondemand_rate
, product_sku
, original_tagcostgroup2 
, original_tagproject 
FROM
  costusage.${environment}_usage.v_awscur_previous_retagbyIDname 
LEFT JOIN (
   SELECT
     type
   , upper(name) name
   , upper(original) original
   , upper(new) new
   FROM
      costusage.source_usage.dim_customtags
   WHERE (type = 'owner_id') -- suggest to switch name and original
)  sa ON owner_id = sa.name
LEFT JOIN (
   SELECT
     type
   , upper(name) name
   , upper(original) original
   , upper(new) new
   FROM
     costusage.source_usage.dim_customtags
   WHERE (type = 'resource_id')
)  re ON ((upper(resource_id) LIKE re.name) AND (upper(owner_id) = re.original)) 
LEFT JOIN (
   SELECT
     type
   , upper(name) name
   , upper(original) original
   , upper(new) new
   FROM
     costusage.source_usage.dim_customtags
   WHERE ((type = 'tags') AND (name = 'CostGroup2'))
)  tg ON (upper(COALESCE(tag_costgroup2, '')) = tg.original)

-- COMMAND ----------

-- DBTITLE 1,rpt view v_fact_awscur_previous
create
or replace VIEW costusage.${environment}_usage.v_fact_awscur_previous as
SELECT
  *
FROM
  costusage.${environment}_usage.v_awscur_previous_reallocated
where
  (
    month = month(
      date_format(dateadd(MONTH, -1, current_date()), 'yyyy-MM-01') 
    )
    and year = year(
      date_format(dateadd(MONTH, -1, current_date()), 'yyyy-MM-01')
    )
  ) 

-- COMMAND ----------

-- DBTITLE 1,rpt view v_fact_awscur_current

create or replace view costusage.${environment}_usage.v_fact_awscur_current
as
SELECT * FROM costusage.${environment}_usage.v_awscur_previous_reallocated
where (month = month(date_format(dateadd(MONTH, 0, current_date()), 'yyyy-MM-01')) and year = year(date_format(dateadd(MONTH, 0, current_date()), 'yyyy-MM-01')))


-- COMMAND ----------

-- MAGIC %md
-- MAGIC # options: select month

-- COMMAND ----------

-- DBTITLE 1,validate parameters from date to date
select year(date_format(dateadd(MONTH, ${from_selected}, current_date()), 'yyyy-MM-01')) * 100 + month(date_format(dateadd(MONTH, ${from_selected}, current_date()), 'yyyy-MM-01')) as selected_from,year(date_format(dateadd(MONTH, ${to_selected}, current_date()), 'yyyy-MM-01')) * 100 + month(date_format(dateadd(MONTH, ${to_selected}, current_date()), 'yyyy-MM-01')) as selected_to

-- COMMAND ----------

-- DBTITLE 1,view v_awscur_selected
-- select date_format(dateadd(MONTH, int(${selected_month}+1), current_date()), 'yyyy-MM-01');
create or replace view costusage.${environment}_usage.v_awscur_selected
as
SELECT
  year
, month
, line_item_usage_start_date usage_time
, bill_billing_entity
, line_item_usage_account_id owner_id
, UPPER(TRIM(resource_tags_user_cost_group1))  tag_costgroup1
, UPPER(TRIM(resource_tags_user_cost_group2))  tag_costgroup2
, UPPER(TRIM(resource_tags_user_name)) tag_name
, UPPER(TRIM(resource_tags_user_s_p)) tag_sp
, resource_tags_user_cluster_id tag_clusterid
, resource_tags_user_cluster_name tag_clustername
, UPPER(TRIM(resource_tags_user_environment)) tag_env
, UPPER(TRIM(resource_tags_user_project)) tag_project
, UPPER(TRIM(resource_tags_user_client)) tag_client
, UPPER(TRIM(resource_tags_user_agency)) tag_agency
, UPPER(TRIM(resource_tags_user_product)) tag_product
, UPPER(TRIM(resource_tags_user_owner)) tag_owner
, UPPER(TRIM(resource_tags_user_scope)) tag_scope
, UPPER(TRIM(resource_tags_user_ambiente)) tag_italyambiente
, UPPER(TRIM(resource_tags_user_cost_group_1)) tag_italycostgroup1
, UPPER(TRIM(resource_tags_user_cost_group_2)) tag_italycostgroup2
, UPPER(TRIM(resource_tags_user_cliente)) tag_italycliente
, (CASE 
WHEN ((bill_billing_entity = 'AWS Marketplace') OR (line_item_product_code like '%Support%')) THEN product_product_name 
WHEN (concat(concat(line_item_resource_id, savings_plan_savings_plan_a_r_n), reservation_reservation_a_r_n) = '') THEN concat('item_id:',identity_line_item_id)
WHEN ((line_item_resource_id = '') AND (reservation_reservation_a_r_n <> '')) THEN concat('ri_arn:',reservation_reservation_a_r_n) 
WHEN ((line_item_resource_id = '') AND (savings_plan_savings_plan_a_r_n <> '')) THEN concat('sp_arn:',savings_plan_savings_plan_a_r_n)
WHEN (line_item_resource_id <> '') THEN line_item_resource_id ELSE line_item_resource_id END) resource_id
, (CASE WHEN (bill_billing_entity = 'AWS Marketplace') THEN bill_billing_entity 
WHEN ((line_item_product_code LIKE '%Savings%') AND (bill_bill_type = 'Purchase')) THEN concat(line_item_product_code, ' Purchase') 
WHEN (line_item_line_item_type = 'Tax') THEN line_item_line_item_type 
WHEN (line_item_product_code like '%Support%') 
THEN product_product_name ELSE line_item_product_code END) aws_service
, product_product_name service_name
, (CASE WHEN ((line_item_product_code LIKE '%Savings%') AND (bill_bill_type = 'Purchase')) THEN 'Purchase' 
ELSE product_product_family END) service_family
, line_item_operation service_operation
, product_region service_region
, product_operating_system service_os
, product_instance_type_family instance_family
, product_instance_type instance_type
, product_marketoption
, product_license_model as license_model
, product_tenancy
, pricing_term
, pricing_unit
, product_purchase_option as purchase_option
, product_database_edition as database_edition
, product_database_engine as database_engine
, product_deployment_option as deployment_option
, line_item_usage_type usage_type
, line_item_line_item_type item_type
, line_item_usage_amount usage_amount
, line_item_net_unblended_cost net_unblended_cost
, pricing_public_on_demand_cost ondemand_cost
, pricing_public_on_demand_rate ondemand_rate
-- , (CASE WHEN (line_item_line_item_type = 'RIFee') THEN line_item_unblended_rate ELSE '0' END) ri_rate
-- , savings_plan_savings_plan_rate sp_rate
-- , reservation_net_effective_cost ri_net_effective_cost
-- , savings_plan_net_savings_plan_effective_cost sp_net_effective_cost
-- , reservation_effective_cost ri_effective_cost
-- , savings_plan_savings_plan_effective_cost sp_effective_cost
, product_sku
FROM
  costusage.source_usage.source_awscur cur
WHERE (1=1)
AND (
  line_item_net_unblended_cost <> 0 OR reservation_net_effective_cost <> 0 OR reservation_net_recurring_fee_for_usage <> 0 OR reservation_net_unused_recurring_fee <> 0 OR savings_plan_net_savings_plan_effective_cost <> 0 OR savings_plan_net_savings_plan_effective_cost <> 0 OR (line_item_net_unblended_cost = 0 AND 
    line_item_line_item_type = 'RIFee'))    
AND line_item_usage_start_date >= date_format(dateadd(MONTH, ${from_selected}, current_date()), 'yyyy-MM-01') 
and 
(
  line_item_usage_start_date < date_format(dateadd(MONTH, int(${to_selected}+1), current_date()), 'yyyy-MM-01')
or (product_product_name = 'Amazon Registrar' and date_format(date(CONCAT(year, '-', LPAD(month, 2, '0'), '-01')), 'yyyy-MM-01') >= date_format(dateadd(MONTH, int(${from_selected}), current_date()), 'yyyy-MM-01') and date_format(date(CONCAT(year, '-', LPAD(month, 2, '0'), '-01')), 'yyyy-MM-01') < date_format(dateadd(MONTH, int(${to_selected}+1), current_date()), 'yyyy-MM-01'))
or line_item_usage_start_date >= date_format(dateadd(MONTH, 0, current_date()), 'yyyy-MM-01')
)



-- COMMAND ----------

select * from costusage.${environment}_usage.v_awscur_selected limit 10

-- COMMAND ----------

-- DBTITLE 1,retagbyid
create 
--MATERIALIZED 
or replace
VIEW costusage.${environment}_usage.v_awscur_selected_retagbyid
as
select 
year
,month
,usage_time
,bill_billing_entity
, owner_id
, first_value(tag_costgroup1) IGNORE NULLS over w as tag_costgroup1
, first_value(tag_costgroup2) IGNORE NULLS over w as tag_costgroup2 
, first_value(tag_name) IGNORE NULLS over w as tag_name 
, first_value(tag_sp) IGNORE NULLS over w as tag_sp 
, tag_clusterid
, tag_clustername
, first_value(tag_env) IGNORE NULLS over w as tag_env 
, first_value(tag_project) IGNORE NULLS over w as tag_project 
, first_value(tag_client) IGNORE NULLS over w as tag_client  
, first_value(tag_agency) IGNORE NULLS over w as tag_agency   
, first_value(tag_product) IGNORE NULLS over w as tag_product 
, first_value(tag_owner) IGNORE NULLS over w as tag_owner 
, first_value(tag_scope) IGNORE NULLS over w as tag_scope  
, first_value(tag_italyambiente) IGNORE NULLS over w as tag_italyambiente   
, first_value(tag_italycostgroup1) IGNORE NULLS over w as tag_italycostgroup1  
, first_value(tag_italycostgroup2) IGNORE NULLS over w as tag_italycostgroup2   
, first_value(tag_italycliente) IGNORE NULLS over w as tag_italycliente  
, resource_id
, aws_service
, service_name
, service_family
, service_operation
, service_region
, service_os
, instance_family
, instance_type
, product_marketoption
, license_model
, product_tenancy
, pricing_term
, pricing_unit
, purchase_option
, database_edition
, database_engine
, deployment_option
, usage_type
, item_type
, usage_amount
, net_unblended_cost
, ondemand_cost
, ondemand_rate
, ri_rate
, sp_rate
, ri_net_effective_cost
, sp_net_effective_cost
, ri_effective_cost
, sp_effective_cost
, tag_costgroup2 as original_tagcostgroup2
, tag_project as original_tagproject
from costusage.${environment}_usage.v_awscur_selected
-- where resource_id ='i-0a869093442623f39'
where (resource_id is not null and resource_id <> "")
WINDOW w as (partition by resource_id order by usage_time desc)
union all 
select *
, tag_costgroup2 as original_tagcostgroup2 
, tag_project as original_tagproject   from costusage.${environment}_usage.v_awscur_selected
where (resource_id is null or resource_id = "")

-- COMMAND ----------

-- DBTITLE 1,retagbyIDname
create 
--MATERIALIZED 
or replace
VIEW costusage.${environment}_usage.v_awscur_selected_retagbyIDname
as 
select 
year
,month
,usage_time
,bill_billing_entity
, owner_id
, first_value(tag_costgroup1) IGNORE NULLS over w as tag_costgroup1
, first_value(tag_costgroup2) IGNORE NULLS over w as tag_costgroup2 
, first_value(tag_name) IGNORE NULLS over w as tag_name 
, first_value(tag_sp) over w as tag_sp 
, tag_clusterid
, tag_clustername
, first_value(tag_env) IGNORE NULLS over w as tag_env 
, first_value(tag_project) IGNORE NULLS over w as tag_project 
, first_value(tag_client) IGNORE NULLS over w as tag_client  
, first_value(tag_agency) IGNORE NULLS over w as tag_agency   
, first_value(tag_product) IGNORE NULLS over w as tag_product 
, first_value(tag_owner) IGNORE NULLS over w as tag_owner
, first_value(tag_scope) IGNORE NULLS over w as tag_scope  
, first_value(tag_italyambiente) IGNORE NULLS over w as tag_italyambiente   
, first_value(tag_italycostgroup1) IGNORE NULLS over w as tag_italycostgroup1  
, first_value(tag_italycostgroup2) IGNORE NULLS over w as tag_italycostgroup2   
, first_value(tag_italycliente) IGNORE NULLS over w as tag_italycliente  
, resource_id
, aws_service
, service_name
, service_family
, service_operation
, service_region
, service_os
, instance_family
, instance_type
, product_marketoption
, license_model
, product_tenancy
, pricing_term
, pricing_unit
, purchase_option
, database_edition
, database_engine
, deployment_option
, usage_type
, item_type
, usage_amount
, net_unblended_cost
, ondemand_cost
, ondemand_rate
, ri_rate
, sp_rate
, ri_net_effective_cost
, sp_net_effective_cost
, ri_effective_cost
, sp_effective_cost
, original_tagcostgroup2 
, original_tagproject 
from costusage.${environment}_usage.v_awscur_selected_retagbyid
where (tag_name is not null and tag_name <> "")
WINDOW w as (partition by tag_name order by usage_time desc)
UNION ALL
select * from costusage.${environment}_usage.v_awscur_selected_retagbyid where (tag_name is null or tag_name ="")


-- COMMAND ----------

-- DBTITLE 1,view reallocated
create 
--MATERIALIZED 
or replace
VIEW costusage.${environment}_usage.v_awscur_selected_reallocated
as 
SELECT
  year,
  month,
  usage_time,
  bill_billing_entity,
  owner_id,
  tag_costgroup1,
  CASE 
    WHEN (sa.original = 'SA') THEN sa.new 
    WHEN (sa.original = 'SCOPE') THEN 
      (CASE WHEN (upper(tag_scope) = 'EMEADATALAKE') THEN 'DATALAKE EMEA' ELSE sa.new END) 
    WHEN (sa.original = 'COSTGROUP2') 
      THEN COALESCE(tg.new, COALESCE(NULLIF(tag_costgroup2, ''), COALESCE(sa.new, ''))) 
      ELSE COALESCE(tg.new, COALESCE(NULLIF(tag_costgroup2, ''), COALESCE(re.new, ''))) END as tag_costgroup2,
  tag_name,
  tag_sp,
  tag_clusterid,
  tag_clustername,
  tag_env,
  tag_project,
  tag_client,
  tag_agency,
  tag_product,
  tag_owner,
  tag_scope,
  tag_italyambiente,
  tag_italycostgroup1,
  tag_italycostgroup2,
  tag_italycliente  , 
  resource_id
, aws_service
, service_name
, service_family
, service_operation
, service_region
, service_os
, instance_family
, instance_type
, product_marketoption
, license_model
, product_tenancy
, pricing_term
, pricing_unit
, purchase_option
, database_edition
, database_engine
, deployment_option
, usage_type
, item_type
, usage_amount
, net_unblended_cost
, ondemand_cost
, ondemand_rate
, ri_rate
, sp_rate
, ri_net_effective_cost
, sp_net_effective_cost
, ri_effective_cost
, sp_effective_cost
, original_tagcostgroup2 
, original_tagproject 
FROM
  costusage.${environment}_usage.v_awscur_selected_retagbyIDname 
LEFT JOIN (
   SELECT
     type
   , upper(name) name
   , upper(original) original
   , upper(new) new
   FROM
      costusage.source_usage.dim_customtags
   WHERE (type = 'owner_id') -- suggest to switch name and original
)  sa ON owner_id = sa.name
LEFT JOIN (
   SELECT
     type
   , upper(name) name
   , upper(original) original
   , upper(new) new
   FROM
     costusage.source_usage.dim_customtags
   WHERE (type = 'resource_id')
)  re ON ((upper(resource_id) LIKE re.name) AND (upper(owner_id) = re.original)) 
LEFT JOIN (
   SELECT
     type
   , upper(name) name
   , upper(original) original
   , upper(new) new
   FROM
     costusage.source_usage.dim_customtags
   WHERE ((type = 'tags') AND (name = 'CostGroup2'))
)  tg ON (upper(COALESCE(tag_costgroup2, '')) = tg.original)

-- COMMAND ----------

-- DBTITLE 1,view v_fact_awscur_selected
create or replace view costusage.${environment}_usage.v_fact_awscur_selected
as
SELECT * FROM costusage.${environment}_usage.v_awscur_selected_reallocated
where ((service_name = 'Amazon Registrar' and date_format(date(CONCAT(year, '-', LPAD(month, 2, '0'), '-01')), 'yyyy-MM-01') < date_format(dateadd(MONTH, int(${to_selected}+1), current_date()), 'yyyy-MM-01') )
or
year*100+month < year(date_format(dateadd(MONTH, ${to_selected}+1, current_date()), 'yyyy-MM-01')) * 100 + month(date_format(dateadd(MONTH, ${to_selected}+1, current_date()), 'yyyy-MM-01')))
and 
 year*100+month >= year(date_format(dateadd(MONTH, ${from_selected}, current_date()), 'yyyy-MM-01')) * 100 + month(date_format(dateadd(MONTH, ${from_selected}, current_date()), 'yyyy-MM-01')) 


-- COMMAND ----------

-- DBTITLE 1,validate selected from date to date
select 'v_fact_awscur_selected',min(year*100+month),max(year*100+month),count(1) from costusage.${environment}_usage.v_fact_awscur_selected
union all
select 'v_awscur_selected',min(year*100+month),max(year*100+month),count(1) from costusage.${environment}_usage.v_awscur_selected

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Aggrate Historical Data

-- COMMAND ----------

-- DBTITLE 1,Table fact_awscur
DROP TABLE IF EXISTS costusage.${environment}_usage.fact_awscur;
create or replace table costusage.${environment}_usage.fact_awscur
using delta
LOCATION 's3://pm-epsilon-athena/databricks/${environment}/${environment}_usage/fact_awscur/'
AS
select * from costusage.${environment}_usage.v_fact_awscur_selected where (1=1);



-- COMMAND ----------

select distinct year*100+month from costusage.${environment}_usage.v_fact_awscur_previous where year*100+month not in (select distinct year*100+month from costusage.${environment}_usage.fact_awscur) and year*100+month <> cast(date_format(current_date(), 'yyyyMM') as int) ;

-- COMMAND ----------

-- DBTITLE 1,archive previous month data
insert into
  costusage.${environment}_usage.fact_awscur
select
  *
from
  costusage.${environment}_usage.v_fact_awscur_previous
where
  year * 100 + month not in (
    select
      distinct year * 100 + month
    from
      costusage.${environment}_usage.fact_awscur
  )
  and year * 100 + month <> cast(date_format(current_date(), 'yyyyMM') as int);

-- COMMAND ----------

-- DBTITLE 1,update resource_id
-- Update tag_costgroup2 in costusage.source_usage.awstags_latest with new from costusage.source_usage.dim_customtags when type is resource_id, resource_id like dim_customtags.name, owner_id is original, and tag_costgroup2 is not new
UPDATE costusage.${environment}_usage.fact_awscur
SET tag_costgroup2 = case when upper(ltrim(rtrim(tag_scope))) ='EMEADATALAKE' then 'DATALAKE EMEA' else upper(ltrim(rtrim(COALESCE(tag_costgroup2,'')))) end
where tag_costgroup2 != upper(ltrim(rtrim(COALESCE(tag_costgroup2,'')))) or (upper(ltrim(rtrim(tag_scope))) ='EMEADATALAKE' and tag_costgroup2 <> 'DATALAKE EMEA');


MERGE INTO costusage.${environment}_usage.fact_awscur AS aws
USING costusage.source_usage.dim_customtags AS dim
   ON aws.owner_id = dim.name
   AND dim.type = 'owner_id' 
   AND dim.original = 'SA'
WHEN MATCHED THEN 
   UPDATE SET aws.tag_costgroup2 = upper(trim(dim.new));


MERGE INTO costusage.${environment}_usage.fact_awscur AS aws
USING costusage.source_usage.dim_customtags AS dim
   ON aws.owner_id = dim.name
   AND dim.type = 'owner_id' 
   AND dim.original = 'CostGroup2'
   AND aws.tag_costgroup2 = ''
WHEN MATCHED THEN 
   UPDATE SET aws.tag_costgroup2 = upper(trim(dim.new));

MERGE INTO costusage.${environment}_usage.fact_awscur AS aws
USING costusage.source_usage.dim_customtags AS dim
   ON aws.tag_costgroup2 = upper(ltrim(rtrim(dim.original)))
   AND dim.type = 'tags' 
   AND dim.name = 'CostGroup2'
   WHEN MATCHED THEN 
   UPDATE SET aws.tag_costgroup2 = upper(trim(dim.new));

MERGE INTO costusage.${environment}_usage.fact_awscur AS aws
USING costusage.source_usage.dim_customtags AS dim
   ON aws.owner_id = dim.original
   AND dim.type = 'resource_id' 
   AND aws.resource_id like dim.name
   WHEN MATCHED THEN 
   UPDATE SET aws.tag_costgroup2 = upper(trim(dim.new));

-- COMMAND ----------

create or replace view costusage.${environment}_usage.v_fact_awscur
as
select * from costusage.${environment}_usage.fact_awscur
union all
select * from costusage.${environment}_usage.v_fact_awscur_current

-- COMMAND ----------

-- DBTITLE 1,athena: previous_month_costgroup2
CREATE OR REPLACE VIEW costusage.${environment}_usage.v_agg_costgroup2_previous
AS 
SELECT
  year
, month
, tag_costgroup2
, (CASE WHEN (bill_billing_entity = 'AWS Marketplace') THEN 'AWS Marketplace' ELSE aws_service END) aws_service
, service_family
, item_type
, sum(net_unblended_cost) sum_net_unblended_cost
FROM
  costusage.${environment}_usage.v_fact_awscur_previous
WHERE (net_unblended_cost <> 0)
GROUP BY year, month, bill_billing_entity, tag_costgroup2, aws_service, service_family, item_type

-- COMMAND ----------

select distinct year*100+month from costusage.${environment}_usage.v_fact_awscur

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Audit retagbyid and awscur

-- COMMAND ----------

-- DBTITLE 1,audit tag reallocate
SELECT date_format(dateadd(Month, -1, current_date()), 'yyyy-MM-01') as startdate,
date_format(dateadd(Month, 0, current_date()), 'yyyy-MM-01') as enddate,
date_format(dateadd(Month, -2, current_date()), 'MM') as register_month,
	'v_fact_awscur_previous' as name,
	count(1) as numofrows
FROM costusage.${environment}_usage.v_fact_awscur_previous 
union
select date_format(dateadd(Month, -1, current_date()), 'yyyy-MM-01') as startdate,
date_format(dateadd(Month, 0, current_date()), 'yyyy-MM-01') as enddate,
date_format(dateadd(Month, -2, current_date()), 'MM') as register_month,
	'source_awscur' as name,
	count(1) as numofrows
FROM costusage.source_usage.source_awscur
 WHERE (1 = 1)
 AND line_item_usage_start_date >= date_format(dateadd(Month, -1, current_date()), 'yyyy-MM-01')
AND month = date_format(dateadd(Month, -1, current_date()), 'MM')
AND (
  line_item_net_unblended_cost <> 0 OR reservation_net_effective_cost <> 0 OR reservation_net_recurring_fee_for_usage <> 0 OR reservation_net_unused_recurring_fee <> 0 OR savings_plan_net_savings_plan_effective_cost <> 0 OR savings_plan_net_savings_plan_effective_cost <> 0 OR (line_item_net_unblended_cost = 0 AND 
	line_item_line_item_type = 'RIFee'))

-- SELECT date_format(dateadd(Month, -1, current_date()), 'yyyy-MM-01') as startdate,
-- date_format(dateadd(Month, 0, current_date()), 'yyyy-MM-01') as enddate,
-- date_format(dateadd(Month, -2, current_date()), 'MM') as register_month,
-- 	'previous_month_cur_reallocated' as name,
-- 	count(1) as numofrows
-- FROM "athenacurcfn_pm_epsilon_cur_athena"."previous_month_cur_reallocated"
-- union

-- COMMAND ----------

-- DBTITLE 1,audit step 1: should be null
select resource_id,owner_id,max(tag_costgroup2),min(tag_costgroup2) from costusage.${environment}_usage.dim_awstags group by resource_id,owner_id having count(1) > 1



-- COMMAND ----------

-- DBTITLE 1,audit 2 should be null
select name,original,max(new),min(new) from costusage.${environment}_usage.dim_customtags where type ='resource_id' group by name,original having count(1) > 1




-- COMMAND ----------

-- DBTITLE 1,audit 3: number match audit inital 0
SELECT date_format(dateadd(Month, -1, current_date()), 'yyyy-MM-01') as startdate,
date_format(dateadd(Month, 0, current_date()), 'yyyy-MM-01') as enddate,
date_format(dateadd(Month, -2, current_date()), 'MM') as register_month,
	'previous_month_aws_cur' as name,
	count(1) as numofrows
FROM costusage.source_usage.previous_aws_cur 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Pending Work - Monthly Data Aggr Run On 10th every Month

-- COMMAND ----------

insert into
  costusage.${environment}_usage.fact_awscur
select
  *
from
  costusage.${environment}_usage.v_fact_awscur_previous
where
  year * 100 + month not in (
    select
      distinct year * 100 + month
    from
      costusage.${environment}_usage.fact_awscur
  )
  and year * 100 + month <> cast(date_format(current_date(), 'yyyyMM') as int);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Pending Work - EC2/RN BENEFIT REALLOCATE
-- MAGIC ## Athena 2 views: ec2 , rn , manupilating work needed
-- MAGIC 1. using SP tag to caculate reserved instances ondemand cost, csp rate to get expected unblended_cost 
-- MAGIC 2. expected net_unblended_cost = (actual ondemand_cost - reserved ondemand_cost) + unblended_cost*0.82
-- MAGIC 3. expected net_unblended_cost - actual net_unblended_cost = adjusted cost
-- MAGIC 4. - adjusted cost to Assets Not Allocated for balance
-- MAGIC     

-- COMMAND ----------

-- DBTITLE 1,ri/sp query investigate
select 
--aws_service
--service_family
--item_type
--usage_type
--"discounted_usage"
--"savingsplan..."
--'RIFEE'

(CASE WHEN (line_item_line_item_type = 'RIFee') THEN line_item_unblended_rate ELSE '0' END) ri_rate
, savings_plan_savings_plan_rate sp_rate
, reservation_net_effective_cost ri_net_effective_cost
, savings_plan_net_savings_plan_effective_cost sp_net_effective_cost
, reservation_effective_cost ri_effective_cost
, savings_plan_savings_plan_effective_cost sp_effective_cost
,*
FROM
  costusage.source_usage.source_awscur cur
WHERE (1=1)
AND (line_item_usage_start_date >= date_format(dateadd(MONTH, ${previous_month}, current_date()), 'yyyy-MM-01'))
AND (
  line_item_net_unblended_cost <> 0 OR reservation_net_effective_cost <> 0 OR reservation_net_recurring_fee_for_usage <> 0 OR reservation_net_unused_recurring_fee <> 0 OR savings_plan_net_savings_plan_effective_cost <> 0 OR savings_plan_net_savings_plan_effective_cost <> 0 OR (line_item_net_unblended_cost = 0 AND 
    line_item_line_item_type = 'RIFee'))


-- COMMAND ----------

CREATE OR REPLACE VIEW costusage.${environment}_usage.v_agg_ec2usage_previous
AS 
SELECT
  year
, month
, (CASE WHEN (tag_costgroup2 <> '') THEN tag_costgroup2 ELSE 'ASSETSNOTALLOCATED' END) tag_costgroup2
, aws_service
, service_family
, item_type
-- start need to recaculate cost for ec2
-- , sum((CASE WHEN (item_type = 'DiscountedUsage') THEN ((ondemand_cost - ri_effective_cost) * 2.5E-1) ELSE 0 END)) unblended_zestyfee
-- , sum((CASE WHEN (item_type = 'DiscountedUsage') THEN ri_net_effective_cost ELSE 0 END)) ri_net_unblended_cost
-- , sum((CASE WHEN (item_type = 'SavingsPlanCoveredUsage') THEN sp_net_effective_cost ELSE 0 END)) sp_net_unblended_cost
-- end
, sum(net_unblended_cost) net_unblended_cost
, sum(ondemand_cost) ondemand_cost
FROM
  costusage.${environment}_usage.v_fact_awscur_previous
WHERE ((((1 = 1) AND (usage_type LIKE '%BoxUsage%')) AND (item_type LIKE '%Usage%')) AND (aws_service = 'AmazonEC2'))
GROUP BY year, month, tag_costgroup2, aws_service, service_family, item_type


-- COMMAND ----------

CREATE OR REPLACE VIEW costusage.${environment}_usage.v_agg_rnusage_previous AS 
SELECT
  year
, month
, DAY(last_day(usage_time)) * 24 as hours_of_month
, tag_costgroup2
, aws_service
, service_family
, item_type
, instance_type
, COALESCE(NULLIF(ondemand_rate, ''), '0') ondemand_rate
-- have no ri_rate --do I need it
-- , COALESCE(NULLIF(ri_rate, ''), '0') ri_rate
-- end
-- no rds condition
, (sum((CASE WHEN ((resource_id LIKE '%reserve%') OR (resource_id LIKE '%938945694778:ri%')) THEN 0 ELSE usage_amount END)) / (DAY(last_day(usage_time)) * 24)) nodes
, (sum((CASE WHEN ((resource_id LIKE '%reserve%') OR (resource_id LIKE '%938945694778:ri%')) THEN usage_amount ELSE 0 END)) / (DAY(last_day(usage_time)) * 24)) ri_nodes
-- end
, sum(net_unblended_cost) net_unblended_cost
, sum(ondemand_cost) ondemand_cost
FROM
   costusage.${environment}_usage.v_fact_awscur_previous
WHERE (((aws_service LIKE '%AmazonRedshift%') AND (service_family IN ('Compute Instance'))) OR ((aws_service LIKE '%AmazonRDS%') AND (service_family IN ('Database Instance'))))
GROUP BY year, month, aws_service, service_family, tag_costgroup2, DAY(last_day(usage_time)) * 24, instance_type, ondemand_rate, item_type


-- COMMAND ----------

-- DBTITLE 1,Query - Get AWS Assets
-- CREATE OR REPLACE VIEW costusage.aws_usage.v_aws_assets AS 
SELECT
--   year
-- , month
-- , DAY(last_day(line_item_usage_start_date)) * 24 hours_of_month
distinct service_family
from costusage.${environment}_usage.v_fact_awscur_previous cur
-- LEFT JOIN costusage.${environment}_usage.aws_skupricing sp ON 
-- cur.product_servicecode = sp.servicecode 
-- AND (cur.product_sku = sp.sku) 
-- AND (cur.product_operating_system = sp.operating_system) 
-- AND (cur.product_region_code = sp.region_code) 
-- AND (cur.product_tenancy = sp.tenancy) 
-- AND (cur.product_license_model = sp.license_model) 
-- AND (cur.product_instance_type = sp.instance_type) AND (cur.line_item_usage_type = sp.usagetype) 
-- AND (cur.product_marketoption = sp.marketoption)
-- and (cur.DataEgine = sp.DataEngine)
-- and (cur.DataEdition = sp.DataEdition)
-- limit 10
WHERE (1 = 1)
and line_item_usage_start_date >= date_format(dateadd(Month, -1, current_date()), 'yyyy-MM-01')
and pricing_term <> 'Spot'
and aws_service IN ('AmazonES', 'AmazonEC2', 'AmazonRDS', 'AmazonElastiCache', 'AmazonRedshift')
and (line_item_line_item_type LIKE '%Usage%' or line_item_usage_type LIKE '%ESInstance%' OR line_item_usage_type LIKE '%Node%')
and (service_family LIKE '%Instance%' OR service_family LIKE '%Serverless%')



-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Query Data

-- COMMAND ----------

-- DBTITLE 1,## Queries for OpenSeach POC Usage
select
  date_format(usage_time, 'yyyy-MM-dd') as usage_date,
  concat(aws_service, '-' ,service_family) as detailed_aws_service,
  pricing_unit,
  resource_id,
  sum(usage_amount) as usage_amount,
  sum(net_unblended_cost) as net_unblended_cost
from
  costusage.aws_usage.v_fact_awscur_current
where
  tag_costgroup2 = 'UF_PAAS'
  and tag_product = 'OPENSEARCH POC'
group by
  aws_service,
  service_family,
  pricing_unit,
  resource_id,
  date_format(usage_time, 'yyyy-MM-dd')

-- COMMAND ----------

-- DBTITLE 1,## Queries for OpenSeach Test/Prod Usage
select
  date_format(usage_time, 'yyyy-MM-dd') as usage_date,
  concat(aws_service, '-' ,service_family) as detailed_aws_service,
  pricing_unit,
  resource_id,
  sum(usage_amount) as usage_amount,
  sum(net_unblended_cost) as net_unblended_cost
from
  costusage.aws_usage.v_fact_awscur_current
where
  tag_costgroup2 = 'UF_PAAS'
  and tag_product = 'OPENSEARCH POC'
group by
  aws_service,
  service_family,
  pricing_unit,
  resource_id,
  date_format(usage_time, 'yyyy-MM-dd')
