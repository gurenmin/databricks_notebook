-- Databricks notebook source
-- MAGIC %md
-- MAGIC # All AWS Assets 

-- COMMAND ----------

-- DBTITLE 1,1. (pending) - pull from unity catlog
refresh table costusage.dev_usage.aws_assets;
select * from costusage.dev_usage.aws_assets;
select * from costusage.source_usage.dim_aws_skupricing;

ALTER TABLE costusage.aws_usage.fact_awscur
SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name');

ALTER TABLE costusage.aws_usage.fact_awscur
DROP COLUMN ri_rate,
 sp_rate,
 ri_net_effective_cost,
 sp_net_effective_cost,
 ri_effective_cost,
 sp_effective_cost,
 original_tagcostgroup2,
 original_tagproject;

ALTER TABLE costusage.aws_usage.fact_awscur
ADD COLUMN (product_sku STRING);

ALTER TABLE costusage.aws_usage.fact_awscur_previous
SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name');

ALTER TABLE costusage.aws_usage.fact_awscur_previous
DROP COLUMN  original_tagcostgroup2,
 original_tagproject;


-- COMMAND ----------

CREATE OR REPLACE VIEW costusage.aws_usage.v_aws_assets AS 
SELECT
  year
, month
, DAY(last_day(usage_time)) * 24 hours_of_month
, usage_time
, owner_id
, tag_costgroup2
, tag_name
, tag_sp
, tag_owner
, tag_clusterid
, tag_clustername
, tag_env
, tag_project
, tag_client
, tag_agency
, tag_product
, tag_scope
, tag_italyambiente
, tag_italycostgroup1
, tag_italycostgroup2
, tag_italycliente
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
, sp.partial_1yr_csp_rate as 1p_csp_rate
, sp.noupfront_1yr_csp_rate as 1n_csp_rate
from costusage.${environment}_usage.fact_awscur_previous cur 
LEFT JOIN costusage.source_usage.dim_aws_skupricing sp ON 
cur.aws_service = sp.product_servicecode 
and cur.service_family = sp.product_product_family
AND (cur.service_os = sp.product_operating_system) 
AND (cur.service_region = sp.product_region_code) 
AND (cur.usage_type = sp.line_item_usage_type) 
-- and (cur.ondemand_rate = sp.pricing_public_on_demand_rate)
WHERE (1 = 1)
-- and cur.usage_time >= date_format(dateadd(Month, -1, current_date()), 'yyyy-MM-01')
and cur.pricing_term <> 'Spot'
and cur.aws_service IN ('AmazonES', 'AmazonEC2', 'AmazonRDS', 'AmazonElastiCache', 'AmazonRedshift')
and (cur.service_family LIKE '%Instance%' OR cur.service_family LIKE '%Serverless%')

-- SELECT
-- count(1)
-- from costusage.${environment}_usage.fact_awscur cur 
-- WHERE (1 = 1)
-- and cur.usage_time >= date_format(dateadd(Month, -1, current_date()), 'yyyy-MM-01')
-- and cur.pricing_term <> 'Spot'
-- and cur.aws_service IN ('AmazonES', 'AmazonEC2', 'AmazonRDS', 'AmazonElastiCache', 'AmazonRedshift')
-- and (cur.service_family LIKE '%Instance%' OR cur.service_family LIKE '%Serverless%')
-- union all
-- SELECT
-- count(1)
-- from costusage.${environment}_usage.fact_awscur cur 
-- LEFT JOIN costusage.source_usage.dim_aws_skupricing sp ON 
-- cur.aws_service = sp.product_servicecode 
-- and cur.service_family = sp.product_product_family
-- AND (cur.service_os = sp.product_operating_system) 
-- AND (cur.service_region = sp.product_region_code) 
-- AND (cur.usage_type = sp.line_item_usage_type) 
-- and (cur.ondemand_rate = sp.pricing_public_on_demand_rate)
-- WHERE (1 = 1)
-- and cur.usage_time >= date_format(dateadd(Month, -1, current_date()), 'yyyy-MM-01')
-- and cur.pricing_term <> 'Spot'
-- and cur.aws_service IN ('AmazonES', 'AmazonEC2', 'AmazonRDS', 'AmazonElastiCache', 'AmazonRedshift')
-- and (cur.service_family LIKE '%Instance%' OR cur.service_family LIKE '%Serverless%')

-- select distinct item_type,usage_type,service_family,pricing_unit,pricing_term
-- from costusage.${environment}_usage.fact_awscur_previous
-- --IDENTIFIER('costusage' || '.' || :environment || '_usage.fact_awscur_previous') 
-- WHERE (1 = 1)
-- and usage_time >= date_format(dateadd(Month, -1, current_date()), 'yyyy-MM-01')
-- -- and pricing_term <> 'Spot'
-- and aws_service IN ('AmazonES', 'AmazonEC2', 'AmazonRDS', 'AmazonElastiCache', 'AmazonRedshift')
-- and (service_family LIKE '%Instance%' OR service_family LIKE '%Serverless%')


-- COMMAND ----------

select * from costusage.aws_usage.v_aws_assets where `1p_csp_rate` is not null

-- COMMAND ----------

-- DBTITLE 1,2. csv pulled from athena - need to add Database Engine and Edition, tag_owner
DROP TABLE costusage.dev_usage.aws_assets
CREATE EXTERNAL TABLE  IF NOT EXISTS costusage.dev_usage.aws_assets
USING csv
OPTIONS (
  'header' 'true',
  'path' 's3://pm-epsilon-athena/databricks/dim_tables/aws_costusage/aws_assets_2024.csv',
  'mergeSchema' 'true'
);
REFRESH TABLE costusage.dev_usage.aws_assets;

-- COMMAND ----------

-- DBTITLE 1,2.1 create silvertable -fact_aws_assets  for performance
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

-- DBTITLE 1,2.2 tag reallocate

update costusage.dev_usage.fact_aws_assets set tag_costgroup2='TITANGERMANY'  where line_item_usage_account_id IN 
('252701498779') and tag_costgroup2 is  null;

update costusage.dev_usage.fact_aws_assets set tag_costgroup2='DTI-US-DATABRICKS' where tag_name ='workerenv-3339421385882115-09795c84-1c27-4c44-914b-7b77b88380d9-worker';

update costusage.dev_usage.fact_aws_assets set tag_costgroup2='DPR-US-DATABRICKS' where tag_name ='workerenv-2838434924534453-f62b61ea-884d-422f-8960-ec22e9f970a7-worker';

update costusage.dev_usage.fact_aws_assets set tag_costgroup2='INFRASTRUCTURE' where tag_name ='arn:aws:rds:us-east-1:003458622776:db:opswise-ue1t';
update costusage.dev_usage.fact_aws_assets set tag_costgroup2='DATALAKE' where tag_name ='arn:aws:redshift-serverless:us-east-1:491258057721:workgroup/74f1bb00-772d-4968-83aa-c768efee5b74';

update costusage.dev_usage.fact_aws_assets set tag_costgroup2='OS_COMMONSERVICES' where tag_costgroup2 ='COMMONSERVICES';
update costusage.dev_usage.fact_aws_assets set tag_costgroup2='OS_AUDIENCE' where tag_costgroup2 ='AUDIENCE';


MERGE INTO costusage.dev_usage.fact_aws_assets AS aws
USING costusage.source_usage.dim_customtags AS dim
   ON aws.line_item_usage_account_id = dim.name
   AND dim.original = 'SA' 
   and dim.type ='owner_id'
WHEN MATCHED THEN 
   UPDATE SET aws.tag_costgroup2 = upper(ltrim(rtrim(dim.new)));

MERGE INTO costusage.dev_usage.fact_aws_assets AS aws
USING costusage.source_usage.dim_customtags AS dim
   ON aws.line_item_usage_account_id = dim.name
   AND dim.original = 'CostGroup2' 
   and dim.type ='owner_id' and aws.tag_costgroup2 is null
WHEN MATCHED THEN 
   UPDATE SET aws.tag_costgroup2 = upper(ltrim(rtrim(dim.new)));

MERGE INTO costusage.dev_usage.fact_aws_assets AS aws
USING costusage.source_usage.dim_customtags AS dim
   ON aws.tag_costgroup2 = dim.original
   AND dim.name = 'CostGroup2' 
   and dim.type ='tags' 
WHEN MATCHED THEN 
   UPDATE SET aws.tag_costgroup2 = upper(ltrim(rtrim(dim.new)));


MERGE INTO costusage.dev_usage.fact_aws_assets AS aws
USING costusage.source_usage.dim_customtags AS dim
   ON aws.resource_id like dim.name
   and aws.line_item_usage_account_id = dim.original
   and dim.type ='resource_id' 
WHEN MATCHED THEN 
   UPDATE SET aws.tag_costgroup2 = upper(ltrim(rtrim(dim.new)));

select * from  costusage.source_usage.dim_customtags

select * from  costusage.source_usage.dim_accounts


-- COMMAND ----------

-- DBTITLE 1,2.3.1 savings rates update for ES,EC,Redshift,OS
-- update costusage.dev_usage.fact_aws_assets set 

update costusage.source_usage.dim_aws_skupricing set partial_1yr_csp_rate =0.124,
noupfront_1yr_csp_rate=0.128
where product_servicecode ='AmazonES'
and product_region_code='us-east-1'
and line_item_usage_type='ESInstance:r5.large'
and pricing_public_on_demand_rate=0.186;

update costusage.dev_usage.fact_aws_assets set partial_1yr_csp_rate =0.124,
noupfront_1yr_csp_rate=0.128
where product_servicecode ='AmazonES'
and product_region_code='us-east-1'
and line_item_usage_type='ESInstance:r5.large'
and pricing_public_on_demand_rate=0.186

-- update costusage.dev_usage.fact_aws_assets set 

update costusage.source_usage.dim_aws_skupricing set partial_1yr_csp_rate =0.102,
noupfront_1yr_csp_rate=0.106,product_operating_system ='NodeUsage:cache.m5.large'
where product_servicecode ='AmazonElastiCache'
and product_region_code='us-east-1'
and line_item_usage_type='NodeUsage:cache.m4.large'
and pricing_public_on_demand_rate=0.156;

update costusage.dev_usage.fact_aws_assets set partial_1yr_csp_rate =0.102,
noupfront_1yr_csp_rate=0.106,product_operating_system ='NodeUsage:cache.m5.large'
where product_servicecode ='AmazonElastiCache'
and product_region_code='us-east-1'
and line_item_usage_type='NodeUsage:cache.m4.large'
and pricing_public_on_demand_rate=0.156;


update costusage.source_usage.dim_aws_skupricing set partial_1yr_csp_rate =0.102,
noupfront_1yr_csp_rate=0.106
where product_servicecode ='AmazonElastiCache'
and product_region_code='us-east-1'
and line_item_usage_type='NodeUsage:cache.t3.micro'
and pricing_public_on_demand_rate=0.017;

update costusage.dev_usage.fact_aws_assets set partial_1yr_csp_rate =0.011,
noupfront_1yr_csp_rate=0.106
where product_servicecode ='AmazonElastiCache'
and product_region_code='us-east-1'
and line_item_usage_type='NodeUsage:cache.t3.micro'
and pricing_public_on_demand_rate=0.017;

update costusage.source_usage.dim_aws_skupricing set partial_1yr_csp_rate =0.044,
noupfront_1yr_csp_rate=0.046
where product_servicecode ='AmazonElastiCache'
and product_region_code='us-east-1'
and line_item_usage_type='NodeUsage:cache.t3.medium'
and pricing_public_on_demand_rate=0.068;

update costusage.dev_usage.fact_aws_assets set partial_1yr_csp_rate =0.044,
noupfront_1yr_csp_rate=0.046
where product_servicecode ='AmazonElastiCache'
and product_region_code='us-east-1'
and line_item_usage_type='NodeUsage:cache.t3.medium'
and pricing_public_on_demand_rate=0.068;

update costusage.source_usage.dim_aws_skupricing set partial_1yr_csp_rate =0.022,
noupfront_1yr_csp_rate=0.023,product_operating_system ='NodeUsage:cache.t3.small'
where product_servicecode ='AmazonElastiCache'
and product_region_code='us-east-1'
and line_item_usage_type='NodeUsage:cache.t2.small'
and pricing_public_on_demand_rate=0.034;

update costusage.dev_usage.fact_aws_assets set partial_1yr_csp_rate =0.022,
noupfront_1yr_csp_rate=0.023,product_operating_system ='NodeUsage:cache.t3.small'
where product_servicecode ='AmazonElastiCache'
and product_region_code='us-east-1'
and line_item_usage_type='NodeUsage:cache.t2.small'
and pricing_public_on_demand_rate=0.034;



update costusage.source_usage.dim_aws_skupricing set partial_1yr_csp_rate =0.134,
noupfront_1yr_csp_rate=0.141
where product_servicecode ='AmazonElastiCache'
and product_region_code='us-east-1'
and line_item_usage_type='NodeUsage:cache.r6g.large'
and pricing_public_on_demand_rate=0.206;

update costusage.dev_usage.fact_aws_assets set partial_1yr_csp_rate =0.134,
noupfront_1yr_csp_rate=0.141
where product_servicecode ='AmazonElastiCache'
and product_region_code='us-east-1'
and line_item_usage_type='NodeUsage:cache.r6g.large'
and pricing_public_on_demand_rate=0.206;

update costusage.source_usage.dim_aws_skupricing set partial_1yr_csp_rate =0.010,
noupfront_1yr_csp_rate=0.011
where product_servicecode ='AmazonElastiCache'
and product_region_code='us-east-1'
and line_item_usage_type='NodeUsage:cache.t4g.micro'
and pricing_public_on_demand_rate=0.016;

update costusage.dev_usage.fact_aws_assets set partial_1yr_csp_rate =0.010,
noupfront_1yr_csp_rate=0.011
where product_servicecode ='AmazonElastiCache'
and product_region_code='us-east-1'
and line_item_usage_type='NodeUsage:cache.t4g.micro'
and pricing_public_on_demand_rate=0.016;


update costusage.source_usage.dim_aws_skupricing set partial_1yr_csp_rate =0.010,
noupfront_1yr_csp_rate=0.011
where product_servicecode ='AmazonElastiCache'
and product_region_code='us-east-1'
and line_item_usage_type='NodeUsage:cache.t4g.micro'
and pricing_public_on_demand_rate=0.016;

update costusage.dev_usage.fact_aws_assets set partial_1yr_csp_rate =0.010,
noupfront_1yr_csp_rate=0.011
where product_servicecode ='AmazonElastiCache'
and product_region_code='us-east-1'
and line_item_usage_type='NodeUsage:cache.t4g.micro'
and pricing_public_on_demand_rate=0.016;


update costusage.source_usage.dim_aws_skupricing set partial_1yr_csp_rate =0.012,
noupfront_1yr_csp_rate=0.013
where product_servicecode ='AmazonElastiCache'
and product_region_code='eu-central-1'
and line_item_usage_type='EUC1-NodeUsage:cache.t3.small'
and pricing_public_on_demand_rate=0.038;

update costusage.dev_usage.fact_aws_assets set partial_1yr_csp_rate =0.012,
noupfront_1yr_csp_rate=0.013
where product_servicecode ='AmazonElastiCache'
and product_region_code='eu-central-1'
and line_item_usage_type='EUC1-NodeUsage:cache.t3.small'
and pricing_public_on_demand_rate=0.038;

update costusage.source_usage.dim_aws_skupricing set partial_1yr_csp_rate =0.170,
noupfront_1yr_csp_rate=0.177
where product_servicecode ='AmazonElastiCache'
and product_region_code='eu-central-1'
and line_item_usage_type='EUC1-NodeUsage:cache.r5.large'
and pricing_public_on_demand_rate=0.26;

update costusage.dev_usage.fact_aws_assets set partial_1yr_csp_rate =0.170,
noupfront_1yr_csp_rate=0.177
where product_servicecode ='AmazonElastiCache'
and product_region_code='eu-central-1'
and line_item_usage_type='EUC1-NodeUsage:cache.r5.large'
and pricing_public_on_demand_rate=0.26;

-- update costusage.dev_usage.fact_aws_assets set 
update costusage.source_usage.dim_aws_skupricing set partial_1yr_csp_rate =0.161,
noupfront_1yr_csp_rate=0.20,product_operating_system ='0.158'
where product_servicecode ='AmazonRedshift'
-- and product_region_code='us-east-1'
and (line_item_usage_type like '%Node:dc2.large')
and pricing_public_on_demand_rate=0.25;

update costusage.dev_usage.fact_aws_assets set partial_1yr_csp_rate =0.161,
noupfront_1yr_csp_rate=0.20,product_operating_system ='0.158'
where product_servicecode ='AmazonRedshift'
-- and product_region_code='us-east-1'
and (line_item_usage_type like '%Node:dc2.large')
and pricing_public_on_demand_rate=0.25;

-- update costusage.dev_usage.fact_aws_assets set 
update costusage.source_usage.dim_aws_skupricing set partial_1yr_csp_rate =0.728,
noupfront_1yr_csp_rate=0.76,product_operating_system ='0.717'
where product_servicecode ='AmazonRedshift'
-- and product_region_code='us-east-1'
and (line_item_usage_type like '%Node:ra3.xlplus')
and pricing_public_on_demand_rate=1.086;

update costusage.dev_usage.fact_aws_assets set partial_1yr_csp_rate =0.728,
noupfront_1yr_csp_rate=0.76,product_operating_system ='0.717'
where product_servicecode ='AmazonRedshift'
-- and product_region_code='us-east-1'
and (line_item_usage_type like '%Node:ra3.xlplus')
and pricing_public_on_demand_rate=1.086;

-- update costusage.dev_usage.fact_aws_assets set 
update costusage.source_usage.dim_aws_skupricing set partial_1yr_csp_rate =2.184,
noupfront_1yr_csp_rate=2.282,product_operating_system ='2.152'
where product_servicecode ='AmazonRedshift'
-- and product_region_code='us-east-1'
and (line_item_usage_type = 'Node:ra3.4xlarge')
and pricing_public_on_demand_rate=3.26;

update costusage.dev_usage.fact_aws_assets set partial_1yr_csp_rate =2.184,
noupfront_1yr_csp_rate=2.282,product_operating_system ='2.152'
where product_servicecode ='AmazonRedshift'
-- and product_region_code='us-east-1'
and (line_item_usage_type = 'Node:ra3.4xlarge')
and pricing_public_on_demand_rate=3.26;

-- update costusage.dev_usage.fact_aws_assets set 
update costusage.source_usage.dim_aws_skupricing set partial_1yr_csp_rate =3.22,
noupfront_1yr_csp_rate=3.8,product_operating_system ='3.155'
where product_servicecode ='AmazonRedshift'
-- and product_region_code='us-east-1'
and (line_item_usage_type = 'Node:dc2.8xlarge')
and pricing_public_on_demand_rate=4.8;

update costusage.dev_usage.fact_aws_assets set partial_1yr_csp_rate =3.22,
noupfront_1yr_csp_rate=3.8,product_operating_system ='3.155'
where product_servicecode ='AmazonRedshift'
-- and product_region_code='us-east-1'
and (line_item_usage_type = 'USE1-Redshift:ServerlessUsage')
and pricing_public_on_demand_rate=4.8;

-- update costusage.dev_usage.fact_aws_assets set 
update costusage.source_usage.dim_aws_skupricing set partial_1yr_csp_rate =0.375,
noupfront_1yr_csp_rate=0.375,product_operating_system ='0.375'
where product_servicecode ='AmazonRedshift'
-- and product_region_code='us-east-1'
and (line_item_usage_type = 'USE1-Redshift:ServerlessUsage')
and pricing_public_on_demand_rate=0.375;

update costusage.dev_usage.fact_aws_assets set partial_1yr_csp_rate =0.375,
noupfront_1yr_csp_rate=0.375,product_operating_system ='0.375'
where product_servicecode ='AmazonRedshift'
-- and product_region_code='us-east-1'
and (line_item_usage_type = 'USE1-Redshift:ServerlessUsage')
and pricing_public_on_demand_rate=0.375;




-- COMMAND ----------

-- DBTITLE 1,2.3.2 savings rates update for RDS

update costusage.source_usage.dim_aws_skupricing set partial_1yr_csp_rate =0.104,
noupfront_1yr_csp_rate=0.110,product_operating_system ='MySQL'
where product_servicecode ='AmazonRDS'
and product_region_code='us-east-1'
and (line_item_usage_type='InstanceUsage:db.m5.large' OR line_item_usage_type='InstanceUsage:db.m6i.large')
and pricing_public_on_demand_rate=0.171;

update costusage.dev_usage.fact_aws_assets set partial_1yr_csp_rate =0.104,
noupfront_1yr_csp_rate=0.110,product_operating_system ='MySQL'
where product_servicecode ='AmazonRDS'
and product_region_code='us-east-1'
and (line_item_usage_type='InstanceUsage:db.m5.large' OR line_item_usage_type='InstanceUsage:db.m6i.large')
and pricing_public_on_demand_rate=0.171;

update costusage.source_usage.dim_aws_skupricing set partial_1yr_csp_rate =0.027,
noupfront_1yr_csp_rate=0.029,product_operating_system ='MySQL'
where product_servicecode ='AmazonRDS'
and product_region_code='eu-central-1'
and (line_item_usage_type='EUC1-InstanceUsage:db.t3.small')
and pricing_public_on_demand_rate=0.04;

update costusage.dev_usage.fact_aws_assets set partial_1yr_csp_rate =0.027,
noupfront_1yr_csp_rate=0.029 ,product_operating_system ='MySQL'
where product_servicecode ='AmazonRDS'
and product_region_code='eu-central-1'
and (line_item_usage_type='EUC1-InstanceUsage:db.t3.small')
and pricing_public_on_demand_rate=0.04;

update costusage.source_usage.dim_aws_skupricing set partial_1yr_csp_rate =0.024,
noupfront_1yr_csp_rate=0.025 ,product_operating_system ='MySQL'
where product_servicecode ='AmazonRDS'
and product_region_code='eu-central-1'
and (line_item_usage_type='EUC1-InstanceUsage:db.t4g.small')
and pricing_public_on_demand_rate=0.037;

update costusage.dev_usage.fact_aws_assets set partial_1yr_csp_rate =0.024,
noupfront_1yr_csp_rate=0.025
where product_servicecode ='AmazonRDS'
and product_region_code='eu-central-1'
and (line_item_usage_type='EUC1-InstanceUsage:db.t4g.small')
and pricing_public_on_demand_rate=0.037;

update costusage.source_usage.dim_aws_skupricing set partial_1yr_csp_rate =0.109,
noupfront_1yr_csp_rate=0.114,product_operating_system ='PostgreSQL'
where product_servicecode ='AmazonRDS'
and product_region_code='us-east-1'
and (line_item_usage_type='InstanceUsage:db.m5.large' OR line_item_usage_type='InstanceUsage:db.m6i.large')
and pricing_public_on_demand_rate=0.178;

update costusage.dev_usage.fact_aws_assets set partial_1yr_csp_rate =0.109,
noupfront_1yr_csp_rate=0.114,product_operating_system ='PostgreSQL'
where product_servicecode ='AmazonRDS'
and product_region_code='us-east-1'
and (line_item_usage_type='InstanceUsage:db.m5.large' OR line_item_usage_type='InstanceUsage:db.m6i.large')
and pricing_public_on_demand_rate=0.178;

update costusage.source_usage.dim_aws_skupricing set partial_1yr_csp_rate =0.098,
noupfront_1yr_csp_rate=0.103,product_operating_system ='PostgreSQL'
where product_servicecode ='AmazonRDS'
and product_region_code='us-east-1'
and (line_item_usage_type='InstanceUsage:db.t3.large')
and pricing_public_on_demand_rate=0.145;

update costusage.dev_usage.fact_aws_assets set partial_1yr_csp_rate =0.098,
noupfront_1yr_csp_rate=0.103,product_operating_system ='PostgreSQL'
where product_servicecode ='AmazonRDS'
and product_region_code='us-east-1'
and (line_item_usage_type='InstanceUsage:db.t3.large')
and pricing_public_on_demand_rate=0.145;

update costusage.source_usage.dim_aws_skupricing set partial_1yr_csp_rate =0.025,
noupfront_1yr_csp_rate=0.026,product_operating_system ='PostgreSQL'
where product_servicecode ='AmazonRDS'
and product_region_code='us-east-1'
and (line_item_usage_type='InstanceUsage:db.t3.small')
and pricing_public_on_demand_rate=0.036;

update costusage.dev_usage.fact_aws_assets set partial_1yr_csp_rate =0.025,
noupfront_1yr_csp_rate=0.026,product_operating_system ='PostgreSQL'
where product_servicecode ='AmazonRDS'
and product_region_code='us-east-1'
and (line_item_usage_type='InstanceUsage:db.t3.small')
and pricing_public_on_demand_rate=0.036;

update costusage.source_usage.dim_aws_skupricing set partial_1yr_csp_rate =0.023,
noupfront_1yr_csp_rate=0.024,product_operating_system ='MySQL'
where product_servicecode ='AmazonRDS'
and product_region_code='us-east-1'
and (line_item_usage_type='InstanceUsage:db.t3.small')
and pricing_public_on_demand_rate=0.034;

update costusage.dev_usage.fact_aws_assets set partial_1yr_csp_rate =0.023,
noupfront_1yr_csp_rate=0.024,product_operating_system ='MySQL'
where product_servicecode ='AmazonRDS'
and product_region_code='us-east-1'
and (line_item_usage_type='InstanceUsage:db.t3.small')
and pricing_public_on_demand_rate=0.034;

update costusage.source_usage.dim_aws_skupricing set partial_1yr_csp_rate =0.046,
noupfront_1yr_csp_rate=0.048,product_operating_system ='MySQL'
where product_servicecode ='AmazonRDS'
and product_region_code='us-east-1'
and ((line_item_usage_type='InstanceUsage:db.t3.medium') or (line_item_usage_type='Multi-AZUsage:db.t3.small'))
and pricing_public_on_demand_rate=0.068;

update costusage.dev_usage.fact_aws_assets set partial_1yr_csp_rate =0.046,
noupfront_1yr_csp_rate=0.048,product_operating_system ='MySQL'
where product_servicecode ='AmazonRDS'
and product_region_code='us-east-1'
and ((line_item_usage_type='InstanceUsage:db.t3.medium') or (line_item_usage_type='Multi-AZUsage:db.t3.small'))
and pricing_public_on_demand_rate=0.068;


update costusage.source_usage.dim_aws_skupricing set partial_1yr_csp_rate =0.011,
noupfront_1yr_csp_rate=0.012,product_operating_system ='MySQL'
where product_servicecode ='AmazonRDS'
and product_region_code like ('us-east-%')
and (line_item_usage_type like '%InstanceUsage:db.t3.micro')
and pricing_public_on_demand_rate=0.017;

update costusage.dev_usage.fact_aws_assets set partial_1yr_csp_rate =0.011,
noupfront_1yr_csp_rate=0.012,product_operating_system ='MySQL'
where product_servicecode ='AmazonRDS'
and product_region_code like ('us-east-%')
and (line_item_usage_type like '%InstanceUsage:db.t3.micro')
and pricing_public_on_demand_rate=0.017;

update costusage.source_usage.dim_aws_skupricing set partial_1yr_csp_rate =0.834,
noupfront_1yr_csp_rate=0.876,product_operating_system ='MySQL'
where product_servicecode ='AmazonRDS'
and product_region_code='us-east-1'
and (line_item_usage_type='InstanceUsage:db.m5.4xl')
and pricing_public_on_demand_rate=1.368;

update costusage.dev_usage.fact_aws_assets set partial_1yr_csp_rate =0.834,
noupfront_1yr_csp_rate=0.876,product_operating_system ='MySQL'
where product_servicecode ='AmazonRDS'
and product_region_code='us-east-1'
and (line_item_usage_type='InstanceUsage:db.m5.4xl')
and pricing_public_on_demand_rate=1.368;


update costusage.source_usage.dim_aws_skupricing set partial_1yr_csp_rate =0.109,
noupfront_1yr_csp_rate=0.114,product_operating_system ='PostgreSQL'
where product_servicecode ='AmazonRDS'
and product_region_code='us-east-1'
and (line_item_usage_type='Multi-AZUsage:db.m4.large')
and pricing_public_on_demand_rate=0.178;

update costusage.dev_usage.fact_aws_assets set partial_1yr_csp_rate =0.109,
noupfront_1yr_csp_rate=0.114,product_operating_system ='PostgreSQL -> m5.large'
where product_servicecode ='AmazonRDS'
and product_region_code='us-east-1'
and (line_item_usage_type='Multi-AZUsage:db.m4.large')
and pricing_public_on_demand_rate=0.178;

update costusage.source_usage.dim_aws_skupricing set partial_1yr_csp_rate =0.022,
noupfront_1yr_csp_rate=0.023,product_operating_system ='MySQL'
where product_servicecode ='AmazonRDS'
and product_region_code='us-east-1'
and (line_item_usage_type='InstanceUsage:db.t4g.small')
and pricing_public_on_demand_rate=0.032;

update costusage.dev_usage.fact_aws_assets set partial_1yr_csp_rate =0.022,
noupfront_1yr_csp_rate=0.023,product_operating_system ='MySQL'
where product_servicecode ='AmazonRDS'
and product_region_code='us-east-1'
and (line_item_usage_type='InstanceUsage:db.t4g.small')
and pricing_public_on_demand_rate=0.032;

update costusage.source_usage.dim_aws_skupricing set partial_1yr_csp_rate =0.012,
noupfront_1yr_csp_rate=0.012,product_operating_system ='PostgreSQL'
where product_servicecode ='AmazonRDS'
and product_region_code like ('us-east-%')
and (line_item_usage_type like '%InstanceUsage:db.t3.micro')
and pricing_public_on_demand_rate=0.018;

update costusage.dev_usage.fact_aws_assets set partial_1yr_csp_rate =0.012,
noupfront_1yr_csp_rate=0.013,product_operating_system ='PostgreSQL'
where product_servicecode ='AmazonRDS'
and product_region_code like ('us-east-%')
and (line_item_usage_type like '%InstanceUsage:db.t3.micro')
and pricing_public_on_demand_rate=0.018;

-- COMMAND ----------

-- DBTITLE 1,3. *get neededsku pricing
create or replace table costusage.source_usage.dim_aws_skupricing
USING DELTA
LOCATION 's3://pm-epsilon-athena/databricks/dim_tables/dim_awssku_pricing'
AS
select
distinct product_servicecode,product_product_family,product_operating_system,product_region_code,line_item_usage_type,partial_1yr_csp_rate,noupfront_1yr_csp_rate,pricing_public_on_demand_rate
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


-- COMMAND ----------

-- DBTITLE 1,Python Tips: insert aws pricing list from api
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

-- DBTITLE 1,get assoicated rn pricing
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

select *  from costusage.aws_usage.reference_rds_pricing where product_region_code ='eu-west-1';
select *  from costusage.source_usage.dim_aws_skupricing where product_region_code ='eu-west-1';
select * from costusage.aws_usage.aws_skupricing where  TermType='OnDemand' and PricePerUnit =0.364 and Region_Code ='us-east-1';
select * from costusage.aws_usage.aws_skupricing where TermType='Reserved'
Multi-AZUsage:db.m4.large

-- COMMAND ----------

select replace(replace(line_item_usage_type,'t2','t3'),'m4','m5'),* from costusage.dev_usage.fact_aws_assets where partial_1yr_csp_rate is null

-- COMMAND ----------

select * from costusage.source_usage.dim_aws_skupricing where partial_1yr_csp_rate is null

-- COMMAND ----------

merge into costusage.dev_usage.fact_aws_assets as target
using costusage.aws_usage.reference_rds_pricing as source
on replace(replace(target.line_item_usage_type,'t2','t3'),'m4','m5')=source.usagetype
and target.pricing_public_on_demand_rate = source.pricing_public_on_demand_rate
and target.product_region_code=source.product_region_code
and target.product_servicecode =source.product_servicecode
and target.product_servicecode ='AmazonRDS'
and source.purchaseoption = 'Partial Upfront'
and source.Database_Engine = target.product_operating_system
and target.partial_1yr_csp_rate is null
WHEN MATCHED THEN UPDATE
SET target.partial_1yr_csp_rate= source.PricePerUnit ;


merge into costusage.dev_usage.fact_aws_assets as target
using costusage.aws_usage.reference_rds_pricing as source
on replace(replace(target.line_item_usage_type,'t2','t3'),'m4','m5')=source.usagetype
and target.pricing_public_on_demand_rate = source.pricing_public_on_demand_rate
and target.product_region_code=source.product_region_code
and target.product_servicecode =source.product_servicecode
and target.product_servicecode ='AmazonRDS'
and source.purchaseoption = 'No Upfront'
and source.Database_Engine = target.product_operating_system
and target.partial_1yr_csp_rate is null
WHEN MATCHED THEN UPDATE
SET target.noupfront_1yr_csp_rate= source.PricePerUnit ;
 
merge into costusage.source_usage.dim_aws_skupricing as target
using costusage.aws_usage.reference_rds_pricing as source
on replace(replace(target.line_item_usage_type,'t2','t3'),'m4','m5')=source.usagetype
and target.pricing_public_on_demand_rate = source.pricing_public_on_demand_rate
and target.product_region_code=source.product_region_code
and target.product_servicecode =source.product_servicecode
and target.product_servicecode ='AmazonRDS'
and source.purchaseoption = 'Partial Upfront'
WHEN MATCHED THEN UPDATE
SET target.partial_1yr_csp_rate= source.PricePerUnit ;
 
merge into costusage.source_usage.dim_aws_skupricing as target
using costusage.aws_usage.reference_rds_pricing as source
on replace(replace(target.line_item_usage_type,'t2','t3'),'m4','m5')=source.usagetype
and target.pricing_public_on_demand_rate = source.pricing_public_on_demand_rate
and target.product_region_code=source.product_region_code
and target.product_servicecode =source.product_servicecode
and target.product_servicecode ='AmazonRDS'
and source.purchaseoption = 'No Upfront'
WHEN MATCHED THEN UPDATE
SET target.noupfront_1yr_csp_rate= source.PricePerUnit ;


select target.*,replace(replace(target.line_item_usage_type,'t2','t3'),'m4','m5'),source.pricing_public_on_demand_rate,source.priceperunit,source.* from
costusage.dev_usage.fact_aws_assets as target
full outer join costusage.aws_usage.reference_rds_pricing as source
on target.line_item_usage_type=source.line_item_usage_type
and target.pricing_public_on_demand_rate = source.pricing_public_on_demand_rate
and target.product_region_code=source.product_region_code
and target.product_servicecode =source.product_servicecode
where target.product_servicecode ='AmazonRDS'
and source.purchaseoption = 'No Upfront'

-- COMMAND ----------

select * from costusage.dev_usage.fact_aws_assets

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # other

-- COMMAND ----------

-- DBTITLE 1,retag via resource_id
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

-- DBTITLE 1,final!!

select 
resource_id
, first_value(tag_costgroup2) IGNORE NULLS over w2 as tag_costgroup2 
, tag_name 
,line_item_usage_start_date
from
(select 
resource_id
, first_value(tag_costgroup2) IGNORE NULLS over w1 as tag_costgroup2 
, first_value(tag_name) IGNORE NULLS over w1 as tag_name 
,line_item_usage_start_date
from costusage.dev_usage.test_tagallocate
where resource_id is not null
WINDOW w1 as (partition by resource_id order by line_item_usage_start_date desc) 
union all 
select 
resource_id
,tag_costgroup2
,tag_name
,line_item_usage_start_date
from costusage.dev_usage.test_tagallocate
where resource_id is null
) a
where tag_name is not null
WINDOW w2 as (partition by tag_name order by line_item_usage_start_date desc) 
union all 
select 
*
from (
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
resource_id
,tag_costgroup2
,tag_name
,line_item_usage_start_date
from costusage.dev_usage.test_tagallocate
where resource_id is null
)
where tag_name is null

-- COMMAND ----------

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

REFRESH TABLE costusage.aws_usage.aws_costusage_2023;
select * from costusage.aws_usage.aws_costusage_2023
REFRESH TABLE costusage.aws_usage.aws_costusage_2024_current

-- COMMAND ----------

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

-- DBTITLE 1,source_awscur
DROP TABLE costusage.source_usage.source_awscur;
CREATE TABLE IF NOT EXISTS costusage.source_usage.source_awscur
USING parquet
OPTIONS (
  'path' 's3://pm-epsilon-cur-athena/pm-cur-athena/pm_epsilon_cur_athena/pm_epsilon_cur_athena',
  'mergeSchema' 'true'
);

-- COMMAND ----------


select *,date_format(date, 'yyyy-MM') as yearmonth from costusage.${environment}_usage.aws_costusage_2024

-- COMMAND ----------

select date_format(dateadd(DAY, 1, current_date()), 'yyyy-MM-dd')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### View Ininital

-- COMMAND ----------

-- DBTITLE 1,view  v_awscur_current
-- select date_format(dateadd(MONTH, int(${selected_month}+1), current_date()), 'yyyy-MM-01');
create or replace view costusage.${environment}_usage.v_awscur_current
as
SELECT
  year
, month
, line_item_usage_start_date
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
, product_instance_type instance_type
, pricing_term
, pricing_unit
, line_item_usage_type usage_type
, line_item_line_item_type item_type
, line_item_usage_amount usage_amount
, line_item_net_unblended_cost net_unblended_cost
, pricing_public_on_demand_cost ondemand_cost
, pricing_public_on_demand_rate ondemand_rate
, (CASE WHEN (line_item_line_item_type = 'RIFee') THEN line_item_unblended_rate ELSE '0' END) ri_rate
, savings_plan_savings_plan_rate sp_rate
, reservation_net_effective_cost ri_net_effective_cost
, savings_plan_net_savings_plan_effective_cost sp_net_effective_cost
, reservation_effective_cost ri_effective_cost
, savings_plan_savings_plan_effective_cost sp_effective_cost
FROM
  costusage.source_usage.source_awscur cur
WHERE (1=1)
AND (
  line_item_net_unblended_cost <> 0 OR reservation_net_effective_cost <> 0 OR reservation_net_recurring_fee_for_usage <> 0 OR reservation_net_unused_recurring_fee <> 0 OR savings_plan_net_savings_plan_effective_cost <> 0 OR savings_plan_net_savings_plan_effective_cost <> 0 OR (line_item_net_unblended_cost = 0 AND 
    line_item_line_item_type = 'RIFee'))
AND line_item_usage_start_date >= date_format(dateadd(MONTH, 0, current_date()), 'yyyy-MM-01') 




-- COMMAND ----------

-- DBTITLE 1,view  v_awscur_previous
create or replace view costusage.${environment}_usage.v_awscur_previous
as
SELECT
  year
, month
, line_item_usage_start_date
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
, product_instance_type instance_type
-- , product_license_model license_model
, pricing_term
, pricing_unit
, line_item_usage_type usage_type
, line_item_line_item_type item_type
, line_item_usage_amount usage_amount
, line_item_net_unblended_cost net_unblended_cost
, pricing_public_on_demand_cost ondemand_cost
, pricing_public_on_demand_rate ondemand_rate
, (CASE WHEN (line_item_line_item_type = 'RIFee') THEN line_item_unblended_rate ELSE '0' END) ri_rate
, savings_plan_savings_plan_rate sp_rate
, reservation_net_effective_cost ri_net_effective_cost
-- , (reservation_net_recurring_fee_for_usage + reservation_net_unused_recurring_fee) ri_net_recurring_fee
, savings_plan_net_savings_plan_effective_cost sp_net_effective_cost
, reservation_effective_cost ri_effective_cost
, savings_plan_savings_plan_effective_cost sp_effective_cost
-- , reservation_amortized_upfront_cost_for_usage ri_amortized_cost
-- , reservation_net_amortized_upfront_cost_for_usage ri_net_amortized_cost
-- , reservation_net_upfront_value
FROM
  costusage.source_usage.source_awscur cur
WHERE (1=1)
AND (line_item_usage_start_date >= date_format(dateadd(MONTH, ${previous_month}, current_date()), 'yyyy-MM-01'))
AND (
  line_item_net_unblended_cost <> 0 OR reservation_net_effective_cost <> 0 OR reservation_net_recurring_fee_for_usage <> 0 OR reservation_net_unused_recurring_fee <> 0 OR savings_plan_net_savings_plan_effective_cost <> 0 OR savings_plan_net_savings_plan_effective_cost <> 0 OR (line_item_net_unblended_cost = 0 AND 
    line_item_line_item_type = 'RIFee'))
-- and (month = month(date_format(dateadd(MONTH, -1, current_date()), 'yyyy-MM-01')) and year = year(date_format(dateadd(MONTH, -1, current_date()), 'yyyy-MM-01')))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC options: select month

-- COMMAND ----------

-- DBTITLE 1,view v_awscur_selected
-- select date_format(dateadd(MONTH, int(${selected_month}+1), current_date()), 'yyyy-MM-01');
create or replace view costusage.${environment}_usage.v_awscur_selected
as
SELECT
  year
, month
, line_item_usage_start_date
, bill_billing_entity
, line_item_usage_account_id owner_id
, resource_tags_user_cost_group1  tag_costgroup1
, resource_tags_user_cost_group2  tag_costgroup2
, resource_tags_user_name tag_name
, resource_tags_user_s_p tag_sp
, resource_tags_user_cluster_id tag_clusterid
, resource_tags_user_cluster_name tag_clustername
, resource_tags_user_environment tag_env
, resource_tags_user_project tag_project
, resource_tags_user_client tag_client
, resource_tags_user_agency tag_agency
, resource_tags_user_product tag_product
, resource_tags_user_scope tag_scope
, resource_tags_user_ambiente tag_italyambiente
, resource_tags_user_cost_group_1 tag_italycostgroup1
, resource_tags_user_cost_group_2 tag_italycostgroup2
, resource_tags_user_cliente tag_italycliente
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
, product_instance_type instance_type
, pricing_term
, pricing_unit
, line_item_usage_type usage_type
, line_item_line_item_type item_type
, line_item_usage_amount usage_amount
, line_item_net_unblended_cost net_unblended_cost
, pricing_public_on_demand_cost ondemand_cost
, pricing_public_on_demand_rate ondemand_rate
, (CASE WHEN (line_item_line_item_type = 'RIFee') THEN line_item_unblended_rate ELSE '0' END) ri_rate
, savings_plan_savings_plan_rate sp_rate
, reservation_net_effective_cost ri_net_effective_cost
, savings_plan_net_savings_plan_effective_cost sp_net_effective_cost
, reservation_effective_cost ri_effective_cost
, savings_plan_savings_plan_effective_cost sp_effective_cost
FROM
  costusage.source_usage.source_awscur cur
WHERE (1=1)
AND (
  line_item_net_unblended_cost <> 0 OR reservation_net_effective_cost <> 0 OR reservation_net_recurring_fee_for_usage <> 0 OR reservation_net_unused_recurring_fee <> 0 OR savings_plan_net_savings_plan_effective_cost <> 0 OR savings_plan_net_savings_plan_effective_cost <> 0 OR (line_item_net_unblended_cost = 0 AND 
    line_item_line_item_type = 'RIFee'))
    
AND line_item_usage_start_date >= date_format(dateadd(MONTH, ${from_selected}, current_date()), 'yyyy-MM-01') 
and 
(line_item_usage_start_date < date_format(dateadd(MONTH, int(${to_selected}+1), current_date()), 'yyyy-MM-01')
or line_item_usage_start_date >= date_format(dateadd(MONTH, 0, current_date()), 'yyyy-MM-01')
)



-- COMMAND ----------

select resource_id,tag_name,tag_costgroup2,line_item_usage_start_date from costusage.${environment}_usage.v_awscur_previous limit 10

-- COMMAND ----------

-- DBTITLE 1,MATERIALIZED VIEW: v_fact_awscur_previous
DROP VIEW costusage.${environment}_usage.v_fact_awscur_previous;
create VIEW costusage.${environment}_usage.v_fact_awscur_previous
as
select * from
(
select year
,month
,line_item_usage_start_date
,bill_billing_entity
, owner_id
, first_value(tag_costgroup1) ignore nulls over w as tag_costgroup1
, first_value(tag_costgroup2) ignore nulls over w as tag_costgroup2 
, first_value(tag_name) ignore nulls over w as tag_name 
, first_value(tag_sp) over w as tag_sp 
, tag_clusterid
, tag_clustername
, first_value(tag_env) over w as tag_env 
, first_value(tag_project) over w as tag_project 
, first_value(tag_client) over w as tag_client  
, first_value(tag_agency) over w as tag_agency   
, first_value(tag_product) over w as tag_product 
, first_value(tag_scope) over w as tag_scope  
, first_value(tag_italyambiente) over w as tag_italyambiente   
, first_value(tag_italycostgroup1) over w as tag_italycostgroup1  
, first_value(tag_italycostgroup2) over w as tag_italycostgroup2   
, first_value(tag_italycliente) over w as tag_italycliente  
, resource_id
, aws_service
, service_name
, service_family
, service_operation
, service_region
, service_os
, instance_type
, pricing_term
, pricing_unit
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
, sp_effective_cost from
(select year
,month
,line_item_usage_start_date
,bill_billing_entity
, owner_id
, first_value(tag_costgroup1) over w as tag_costgroup1
, first_value(tag_costgroup2) over w as tag_costgroup2 
, first_value(tag_name) over w as tag_name 
, first_value(tag_sp) over w as tag_sp 
, tag_clusterid
, tag_clustername
, first_value(tag_env) over w as tag_env 
, first_value(tag_project) over w as tag_project 
, first_value(tag_client) over w as tag_client  
, first_value(tag_agency) over w as tag_agency   
, first_value(tag_product) over w as tag_product 
, first_value(tag_scope) over w as tag_scope  
, first_value(tag_italyambiente) over w as tag_italyambiente   
, first_value(tag_italycostgroup1) over w as tag_italycostgroup1  
, first_value(tag_italycostgroup2) over w as tag_italycostgroup2   
, first_value(tag_italycliente) over w as tag_italycliente  
, resource_id
, aws_service
, service_name
, service_family
, service_operation
, service_region
, service_os
, instance_type
, pricing_term
, pricing_unit
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
from costusage.${environment}_usage.v_awscur_previous
WINDOW w as (partition by resource_id order by line_item_usage_start_date desc)
) a
WINDOW w as (partition by tag_name order by line_item_usage_start_date desc)
) b
where (month = month(date_format(dateadd(MONTH, ${previous_month}, current_date()), 'yyyy-MM-01')) and year = year(date_format(dateadd(MONTH, ${previous_month}, current_date()), 'yyyy-MM-01')))

-- COMMAND ----------

-- DBTITLE 1,view v_fact_awscur_current

create or replace view costusage.${environment}_usage.v_fact_awscur_current
as
select * from
(
select year
,month
,line_item_usage_start_date
,bill_billing_entity
, owner_id
, first_value(tag_costgroup1) over w as tag_costgroup1
, first_value(tag_costgroup2) over w as tag_costgroup2 
, first_value(tag_name) over w as tag_name 
, first_value(tag_sp) over w as tag_sp 
, tag_clusterid
, tag_clustername
, first_value(tag_env) over w as tag_env 
, first_value(tag_project) over w as tag_project 
, first_value(tag_client) over w as tag_client  
, first_value(tag_agency) over w as tag_agency   
, first_value(tag_product) over w as tag_product 
, first_value(tag_scope) over w as tag_scope  
, first_value(tag_italyambiente) over w as tag_italyambiente   
, first_value(tag_italycostgroup1) over w as tag_italycostgroup1  
, first_value(tag_italycostgroup2) over w as tag_italycostgroup2   
, first_value(tag_italycliente) over w as tag_italycliente  
, resource_id
, aws_service
, service_name
, service_family
, service_operation
, service_region
, service_os
, instance_type
, pricing_term
, pricing_unit
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
, sp_effective_cost from
(select year
,month
,line_item_usage_start_date
,bill_billing_entity
, owner_id
, first_value(tag_costgroup1) over w as tag_costgroup1
, first_value(tag_costgroup2) over w as tag_costgroup2 
, first_value(tag_name) over w as tag_name 
, first_value(tag_sp) over w as tag_sp 
, tag_clusterid
, tag_clustername
, first_value(tag_env) over w as tag_env 
, first_value(tag_project) over w as tag_project 
, first_value(tag_client) over w as tag_client  
, first_value(tag_agency) over w as tag_agency   
, first_value(tag_product) over w as tag_product 
, first_value(tag_scope) over w as tag_scope  
, first_value(tag_italyambiente) over w as tag_italyambiente   
, first_value(tag_italycostgroup1) over w as tag_italycostgroup1  
, first_value(tag_italycostgroup2) over w as tag_italycostgroup2   
, first_value(tag_italycliente) over w as tag_italycliente  
, resource_id
, aws_service
, service_name
, service_family
, service_operation
, service_region
, service_os
, instance_type
, pricing_term
, pricing_unit
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
from costusage.${environment}_usage.v_awscur_previous
-- where resource_id ='i-0a869093442623f39'
WINDOW w as (partition by resource_id order by line_item_usage_start_date desc)
) a
WINDOW w as (partition by tag_name order by line_item_usage_start_date desc)
) b
where (month = month(date_format(dateadd(MONTH, 0, current_date()), 'yyyy-MM-01')) and year = year(date_format(dateadd(MONTH, 0, current_date()), 'yyyy-MM-01')))

-- COMMAND ----------

-- DBTITLE 1,#temp table fact_awscur_previous
DROP TABLE IF EXISTS costusage.${environment}_usage.fact_awscur_previous;
create or replace table costusage.${environment}_usage.fact_awscur_previous
using delta
LOCATION 's3://pm-epsilon-athena/databricks/${environment}/${environment}_usage/fact_awscur_previous/'
AS
select * from costusage.${environment}_usage.v_fact_awscur_previous;


-- COMMAND ----------

-- DBTITLE 1,view v_fact_awscur_selected

create or replace view costusage.${environment}_usage.v_fact_awscur_selected
as
select * from
(
select year
,month
,line_item_usage_start_date
,bill_billing_entity
, owner_id
, first_value(tag_costgroup1) over w as tag_costgroup1
, first_value(tag_costgroup2) over w as tag_costgroup2 
, first_value(tag_name) over w as tag_name 
, first_value(tag_sp) over w as tag_sp 
, tag_clusterid
, tag_clustername
, first_value(tag_env) over w as tag_env 
, first_value(tag_project) over w as tag_project 
, first_value(tag_client) over w as tag_client  
, first_value(tag_agency) over w as tag_agency   
, first_value(tag_product) over w as tag_product 
, first_value(tag_scope) over w as tag_scope  
, first_value(tag_italyambiente) over w as tag_italyambiente   
, first_value(tag_italycostgroup1) over w as tag_italycostgroup1  
, first_value(tag_italycostgroup2) over w as tag_italycostgroup2   
, first_value(tag_italycliente) over w as tag_italycliente  
, resource_id
, aws_service
, service_name
, service_family
, service_operation
, service_region
, service_os
, instance_type
, pricing_term
, pricing_unit
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
, sp_effective_cost from
(select year
,month
,line_item_usage_start_date
,bill_billing_entity
, owner_id
, first_value(tag_costgroup1) over w as tag_costgroup1
, first_value(tag_costgroup2) over w as tag_costgroup2 
, first_value(tag_name) over w as tag_name 
, first_value(tag_sp) over w as tag_sp 
, tag_clusterid
, tag_clustername
, first_value(tag_env) over w as tag_env 
, first_value(tag_project) over w as tag_project 
, first_value(tag_client) over w as tag_client  
, first_value(tag_agency) over w as tag_agency   
, first_value(tag_product) over w as tag_product 
, first_value(tag_scope) over w as tag_scope  
, first_value(tag_italyambiente) over w as tag_italyambiente   
, first_value(tag_italycostgroup1) over w as tag_italycostgroup1  
, first_value(tag_italycostgroup2) over w as tag_italycostgroup2   
, first_value(tag_italycliente) over w as tag_italycliente  
, resource_id
, aws_service
, service_name
, service_family
, service_operation
, service_region
, service_os
, instance_type
, pricing_term
, pricing_unit
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
from costusage.${environment}_usage.v_awscur_previous
-- where resource_id ='i-0a869093442623f39'
WINDOW w as (partition by resource_id order by line_item_usage_start_date desc)
) a
WINDOW w as (partition by tag_name order by line_item_usage_start_date desc)
) b
where 
 year*100+month >= year(date_format(dateadd(MONTH, ${from_selected}, current_date()), 'yyyy-MM-01')) * 100 + month(date_format(dateadd(MONTH, ${from_selected}, current_date()), 'yyyy-MM-01'))



-- COMMAND ----------

-- DBTITLE 1,validate selected from date to date
select year(date_format(dateadd(MONTH, ${from_selected}, current_date()), 'yyyy-MM-01')) * 100 + month(date_format(dateadd(MONTH, ${from_selected}, current_date()), 'yyyy-MM-01')) as selected_from,year(date_format(dateadd(MONTH, ${to_selected}, current_date()), 'yyyy-MM-01')) * 100 + month(date_format(dateadd(MONTH, ${to_selected}, current_date()), 'yyyy-MM-01')) as selected_to

-- COMMAND ----------

-- DBTITLE 1,validate
select tag_project,max(line_item_usage_start_date),min(line_item_usage_start_date) 
from costusage.${environment}_usage.v_fact_awscur_current where resource_id ='i-0a869093442623f39'
group by tag_project

-- COMMAND ----------

-- DBTITLE 1,validate
-- REFRESH MATERIALIZED VIEW costusage.${environment}_usage.v_fact_awscur_previous;
select * from costusage.${environment}_usage.v_fact_awscur_previous where tag_costgroup2 ='PRECISION'
and tag_project is null

-- COMMAND ----------

-- DBTITLE 1,Table fact_awscur
DROP TABLE IF EXISTS costusage.${environment}_usage.fact_awscur;
create or replace table costusage.${environment}_usage.fact_awscur
using delta
LOCATION 's3://pm-epsilon-athena/databricks/${environment}/${environment}_usage/fact_awscur/'
AS
select * from costusage.${environment}_usage.v_fact_awscur_previous where (1=0) ;



-- COMMAND ----------

delete from costusage.${environment}_usage.fact_awscur where year*100+month in (select distinct year*100+month from costusage.${environment}_usage.fact_awscur_previous);

-- COMMAND ----------

-- DBTITLE 1,archive previous month data

insert into costusage.${environment}_usage.fact_awscur
select * from costusage.${environment}_usage.fact_awscur_previous where year*100+month not in (select distinct year*100+month from costusage.${environment}_usage.fact_awscur);

-- COMMAND ----------

create or replace view costusage.${environment}_usage.v_fact_awscur
as
select * from costusage.${environment}_usage.fact_awscur
union all
select * from costusage.${environment}_usage.v_fact_awscur_current

-- COMMAND ----------

SELECT * FROM costusage.${environment}_usage.v_fact_awscur LIMIT 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Audit

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

-- DBTITLE 1,aggr_browntable: previous_aws_cur, may change to env_usage
DROP MATERIALIZED VIEW IF Exists costusage.${environment}_usage.v_aggr_awscur_previous;
create MATERIALIZED VIEW costusage.${environment}_usage.v_aggr_awscur_previous
as
select 
year,
month,
date_format(dateadd(Month, -1, current_date()), 'yyyy-MM-01') as line_item_usage_start_date,
bill_billing_entity,
owner_id,
upper(tag_costgroup1) as tag_costgroup1,
upper(tag_costgroup2) as tag_costgroup2,
tag_name,
tag_sp,
tag_clusterid,
tag_clustername,
upper(tag_env) as tag_env,
tag_project,
tag_client,
tag_agency,
tag_product,
upper(tag_scope) as tag_scope,
tag_italyambiente,
tag_italycliente,
tag_italycostgroup1,
tag_italycostgroup2,
resource_id,
aws_service,
service_name,
service_family,
service_operation,
service_region,
service_os,
instance_type,
pricing_term,
pricing_unit,
usage_type,
item_type,
sum(usage_amount) as usage_amount,
sum(net_unblended_cost) as net_unblended_cost,
sum(ondemand_cost) as ondemand_cost,
ondemand_rate,
ri_rate,
sp_rate,
sum(ri_net_effective_cost) as ri_net_effective_cost,
sum(sp_net_effective_cost) as sp_net_effective_cost,
sum(ri_effective_cost) as ri_effective_cost,
sum(sp_effective_cost) as sp_effective_cost
from costusage.${environment}_usage.v_fact_awscur_previous
where ((upper(pricing_unit) not like '%HR%' and upper(pricing_unit) not like '%HOUR%') or usage_amount > 1)
group by
year,
month,
bill_billing_entity,
owner_id,
upper(tag_costgroup1),
upper(tag_costgroup2),
tag_name,
tag_sp,
tag_clusterid,
tag_clustername,
upper(tag_env),
tag_project,
tag_client,
tag_agency,
tag_product,
tag_italyambiente,
tag_italycliente,
tag_italycostgroup1,
tag_italycostgroup2,
upper(tag_scope), 
resource_id,
aws_service,
service_name,
service_family,
service_operation,
service_region,
service_os,
instance_type,
pricing_term,
pricing_unit,
usage_type,
item_type,
ondemand_rate,
ri_rate,
sp_rate
union all
select * from costusage.${environment}_usage.v_fact_awscur_previous
where (
 upper(pricing_unit) like '%HR%'
or upper(pricing_unit) like '%HOUR%'
)
and usage_amount <= 1

--16415762
--1941694
--14474068
REFRESH MATERIALIZED VIEW costusage.${environment}_usage.v_fact_awscur_previous;

-- COMMAND ----------

 select * from costusage.${environment}_usage.v_aggr_awscur_previous 
 where tag_costgroup2 = 'PRECISION'

-- COMMAND ----------

-- DBTITLE 1,update resource_id
-- Update tag_costgroup2 in costusage.source_usage.awstags_latest with new from costusage.source_usage.dim_customtags when type is resource_id, resource_id like dim_customtags.name, owner_id is original, and tag_costgroup2 is not new
MERGE INTO costusage.${environment}_usage.fact_awscur AS aws
USING costusage.source_usage.dim_awstags AS dim
   ON aws.resource_id = dim.resource_id
   AND aws.owner_id = dim.owner_id
WHEN MATCHED THEN 
   UPDATE SET aws.tag_costgroup2 = upper(ltrim(rtrim(dim.tag_costgroup2)));

UPDATE costusage.${environment}_usage.fact_awscur
SET tag_costgroup2 = case when upper(ltrim(rtrim(tag_scope))) ='EMEADATALAKE' then 'DATALAKE EMEA' else upper(ltrim(rtrim(COALESCE(tag_costgroup2,'')))) end
where tag_costgroup2 != upper(ltrim(rtrim(COALESCE(tag_costgroup2,'')))) or (upper(ltrim(rtrim(tag_scope))) ='EMEADATALAKE' and tag_costgroup2 <> 'DATALAKE EMEA');




-- COMMAND ----------

-- DBTITLE 1,audit step 1: should be null
select resource_id,owner_id,max(tag_costgroup2),min(tag_costgroup2) from costusage.source_usage.dim_awstags group by resource_id,owner_id having count(1) > 1



-- COMMAND ----------

-- DBTITLE 1,inital latest tag costgroup2
MERGE INTO costusage.${environment}_usage.previous_aws_cur AS aws
USING costusage.source_usage.dim_awstags AS dim
   ON aws.resource_id = dim.resource_id
   AND aws.owner_id = dim.owner_id
WHEN MATCHED THEN 
   UPDATE SET aws.tag_costgroup2 = upper(ltrim(rtrim(dim.tag_costgroup2)));

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

MERGE INTO costusage.${environment}_usage.fact_aws_cur AS aws
USING costusage.${environment}_usage.dim_customtags AS dim
   ON aws.owner_id = dim.name
   AND dim.type = 'owner_id' 
   AND dim.original = 'SA'
WHEN MATCHED THEN 
   UPDATE SET aws.tag_costgroup2 = upper(ltrim(rtrim(dim.new)));

-- COMMAND ----------

MERGE INTO costusage.${environment}_usage.previous_aws_cur AS aws
USING costusage.${environment}_usage.dim_customtags AS dim
   ON aws.owner_id = dim.name
   AND dim.type = 'owner_id' 
   AND dim.original = 'CostGroup2'
   AND aws.tag_costgroup2 = ''
WHEN MATCHED THEN 
   UPDATE SET aws.tag_costgroup2 = upper(ltrim(rtrim(dim.new)));

-- COMMAND ----------

MERGE INTO costusage.${environment}_usage.previous_aws_cur AS aws
USING costusage.${environment}_usage.dim_customtags AS dim
   ON aws.tag_costgroup2 = upper(ltrim(rtrim(dim.original)))
   AND dim.type = 'tags' 
   AND dim.name = 'CostGroup2'
   WHEN MATCHED THEN 
   UPDATE SET aws.tag_costgroup2 = upper(ltrim(rtrim(dim.new)));

-- COMMAND ----------

MERGE INTO costusage.${environment}_usage.previous_aws_cur AS aws
USING costusage.${environment}_usage.dim_customtags AS dim
   ON aws.owner_id = dim.original
   AND dim.type = 'resource_id' 
   AND aws.resource_id like dim.name
   WHEN MATCHED THEN 
   UPDATE SET aws.tag_costgroup2 = upper(ltrim(rtrim(dim.new)));

-- COMMAND ----------

UPDATE costusage.${environment}_usage.previous_aws_cur
SET tag_costgroup2 = upper(ltrim(rtrim(COALESCE(tag_costgroup2,'')))) 
where tag_costgroup2 <> upper(ltrim(rtrim(COALESCE(tag_costgroup2,''))))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### TAG REALLOCATED DONE
-- MAGIC
-- MAGIC #### EC2/RN BENEFIT REALLOCATE

-- COMMAND ----------

-- DBTITLE 1,EC2/rn


-- COMMAND ----------



-- COMMAND ----------

-- DBTITLE 1,SILVER TABLE READY
select * from costusage.${environment}_usage.previous_aws_cur where tag_costgroup2 ='VIVAKI ITALY'

-- COMMAND ----------

-- DBTITLE 1,Italy
create or replace view costusage.${environment}_usage.v_italy_detailed_usage
as
select 
year,
month,
date_format(dateadd(Month, 0, line_item_usage_start_date), 'yyyy-MM-01') as date,
bill_billing_entity,
'Agency Cross Charge' as tag_costgroup1,
tag_costgroup2,
COALESCE(tag_name,'') as tag_name,
case when COALESCE(tag_name,'') = ''  then resource_id,
tag_sp,
max(tag_italyambiente) as tag_italyambiente,
max(tag_italycliente) as tag_italycliente,
max(tag_italycostgroup1) as tag_italycostgroup1,
max(tag_italycostgroup2) as tag_italycostgroup2,
max(tag_product) as tag_italyproduct,
aws_service,
service_family,
service_region,
service_os,
instance_type,
pricing_term,
pricing_unit,
ondemand_rate,
sum(usage_amount) as usage_amount,
sum(ondemand_cost)*0.82 as net_ondemand_cost
from costusage.${environment}_usage.previous_aws_cur 
where upper(tag_costgroup2) ='VIVAKI ITALY' 
and item_type <> 'Tax'
group by year,
month,
date_format(dateadd(Month, 0, line_item_usage_start_date), 'yyyy-MM-01'),
bill_billing_entity,
tag_costgroup2,
COALESCE(tag_name,''),
tag_sp,
aws_service,
service_family,
service_region,
service_os,
instance_type,
pricing_term,
pricing_unit,
ondemand_rate,
resource_id
having net_ondemand_cost != 0

-- COMMAND ----------

-- Create a delta table with partition based on line_item_usage_start_date
CREATE TABLE costusage.${environment}_usage.fact_awscur
USING DELTA
PARTITIONED BY (year, month)
LOCATION 's3://pm-epsilon-athena/databricks/${environment}/${environment}_usage/awscur/'
AS
SELECT * FROM costusage.${environment}_usage.previous_aws_cur where 0 = 1;

-- COMMAND ----------

-- DBTITLE 1,steam load
select * from costusage.${environment}_usage.fact_awscur

-- COMMAND ----------

-- DBTITLE 1,bill_awscur
--pivot data

-- COMMAND ----------

drop view if exists joined;

create temporary view joined as
select aws.type,aws.resource_id,aws.owner_id, CASE WHEN tag_costgroup2 = upper(ltrim(rtrim(dim.new))) THEN tag_costgroup2 ELSE upper(ltrim(rtrim(dim.new))) END as tag_costgroup2
from costusage.source_usage.awstags_latest as aws inner join costusage.source_usage.dim_customtags as dim 
ON aws.type=dim.type
AND aws.resource_id LIKE dim.name
AND aws.owner_id = dim.original
where dim.type='resource_id';

merge into costusage.source_usage.awstags_latest as aws
using joined ON aws.type=joined.type AND aws.resource_id =joined.resource_id AND aws.owner_id = joined.owner_id
WHEN MATCHED THEN UPDATE set tag_costgroup2 = joined.tag_costgroup2;

-- COMMAND ----------

select * from costusage.source_usage.v_previous_month_aws_cur limit 10

-- COMMAND ----------

-- DBTITLE 1,update account not in awstags_latest
merge into costusage.source_usage.awstags_latest as aws
using costusage.source_usage.dim_customtags as dim 
ON  dim.type = 'owner_id'
AND aws.owner_id = dim.name 
AND aws.tag_costgroup2 =''
WHEN MATCHED THEN UPDATE set aws.tag_costgroup2 = dim.new;

-- COMMAND ----------



select * from costusage.source_usage.v_previous_month_aws_cur aws
left join costusage.source_usage.awstags_latest tag
group by year,month,bill_billing_entity,owner_id,tag_costgroup1,tag_costgroup2 limit 10

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC from pyspark.sql import SparkSession
-- MAGIC from datetime import datetime, timedelta
-- MAGIC
-- MAGIC # Create SparkSession
-- MAGIC spark = SparkSession.builder \
-- MAGIC     .appName("example-app") \
-- MAGIC     .getOrCreate()
-- MAGIC
-- MAGIC # Read the data from the table
-- MAGIC df = spark.table("costusage.source_usage.v_previous_month_aws_cur")
-- MAGIC
-- MAGIC # Read the data from the table
-- MAGIC tag_df = spark.table("costusage.source_usage.awstags_latest")
-- MAGIC
-- MAGIC # Update tag_costgroup2 in df to match tag_costgroup2 in tag_df
-- MAGIC df = df.join(tag_df, on=['tag_costgroup2'], how='left')
-- MAGIC df = df.drop('tag_costgroup2')
-- MAGIC df = df.withColumnRenamed('tag_costgroup2', 'tag_costgroup2_tag_df')
-- MAGIC df = df.withColumnRenamed('tag_costgroup2_tag_df', 'tag_costgroup2')
-- MAGIC
-- MAGIC

-- COMMAND ----------

VACUUM costusage.${environment}_usage.fact_awsusage

DROP TABLE costusage.${environment}_usage.fact_awsusage



-- COMMAND ----------

CREATE OR REPLACE TABLE costusage.${environment}_usage.fact_awsusage
USING DELTA
LOCATION 's3://pm-epsilon-athena/databricks/${environment}/${environment}_usage/fact_awsusage/'
PARTITIONED BY (year , month)
select * from costusage.source_costusage.v_current_month_aws_cur

insert table costusage.${environment}_usage.fact_awsusage
select * from costusage.source_costusage.v_previous_month_aws_cur

-- COMMAND ----------

-- MAGIC %md
-- MAGIC EC2 Usage

-- COMMAND ----------

select * from costusage.${environment}_usage.fact_awscur

-- COMMAND ----------


SELECT
  year
, month
, (CASE WHEN (tag_costgroup2 <> '') THEN tag_costgroup2 ELSE 'ASSETSNOTALLOCATED' END) tag_costgroup2
, aws_service
, service_family
, item_type
, sum((CASE WHEN (item_type = 'DiscountedUsage') THEN ((ondemand_cost - ri_effective_cost) * 0.25) ELSE 0 END)) unblended_zestyfee
, sum((CASE WHEN (item_type = 'DiscountedUsage') THEN ri_net_effective_cost ELSE 0 END)) ri_net_unblended_cost
, sum((CASE WHEN (item_type = 'SavingsPlanCoveredUsage') THEN sp_net_effective_cost ELSE 0 END)) sp_net_unblended_cost
, sum(net_unblended_cost) net_unblended_cost
, sum(ondemand_cost) ondemand_cost
FROM
  costusage.${environment}_usage.fact_awsusage_previous
WHERE ((((1 = 1) AND (usage_type LIKE '%BoxUsage%')) AND (item_type LIKE '%Usage%')) AND (aws_service = 'AmazonEC2'))
GROUP BY year, month, tag_costgroup2, aws_service, service_family, item_type
