-- Databricks notebook source
-- DBTITLE 1,widgets for environment
-- MAGIC %python
-- MAGIC # dbutils.widgets.help("dropdown")
-- MAGIC # dbutils.widgets.removeAll()
-- MAGIC # dbutils.widgets.dropdown("environment", "dev", ["dev", "test", "prod"])
-- MAGIC

-- COMMAND ----------

-- DBTITLE 1,validate min usage_date check from system.billing.usage
-- MAGIC %sql select min(usage_date) FROM system.billing.usage

-- COMMAND ----------

-- DBTITLE 1,Create Catalog
CREATE CATALOG if not exists costusage
MANAGED LOCATION 's3://pm-epsilon-athena/databricks/';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC source_usage database

-- COMMAND ----------

-- DBTITLE 1,Create source_usage Database [Schema under Catalog]
CREATE DATABASE IF NOT EXISTS costusage.source_usage
MANAGED LOCATION 's3://pm-epsilon-athena/databricks/source/';

-- COMMAND ----------

-- DBTITLE 1,Create external dim tables
DROP Table costusage.source_usage.dim_customtags;
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

CREATE EXTERNAL TABLE  IF NOT EXISTS costusage.source_usage.dim_contracts
USING csv
OPTIONS (
  'header' 'true',
  'path' 's3://pm-epsilon-athena/databricks/dim_tables/dim_contracts/dim_contracts.csv',
  'mergeSchema' 'true'
);

DROP Table costusage.source_usage.dim_customtags;
CREATE EXTERNAL TABLE  costusage.source_usage.dim_workspaces
USING csv
OPTIONS (
  'header' 'true',
  'path' 's3://pm-epsilon-athena/databricks/dim_tables/dim_workspaces/dim_workspaces.csv',
  'mergeSchema' 'true'
);


CREATE EXTERNAL TABLE  IF NOT EXISTS costusage.source_usage.dim_skupricing
USING csv
OPTIONS (
  'header' 'true',
  'path' 's3://pm-epsilon-athena/databricks/dim_tables/dim_skupricing/dim_skupricing.csv',
  'mergeSchema' 'true'
);

-- /CREATE TABLE IF NOT EXISTS costusage.dev_usage.fact_zestyri
-- USING csv
-- OPTIONS (
--   'header' 'true',
--   'path' 's3://pm-epsilon-athena/databricks/dev/dev_usage/cms/Nov_RI.csv',
--   'mergeSchema' 'true'
-- );
REFRESH TABLE costusage.source_usage.dim_customtags;
REFRESH TABLE costusage.source_usage.dim_contracts;
REFRESH TABLE costusage.source_usage.dim_workspaces;
REFRESH TABLE costusage.source_usage.dim_skupricing;

-- COMMAND ----------

-- DBTITLE 1,Create table for dbutags if not exists
--DROP TABLE costusage.source_usage.dim_dbutags;
CREATE TABLE IF NOT EXISTS costusage.source_usage.dim_dbutags
USING delta
LOCATION 's3://pm-epsilon-athena/databricks/dim_tables/dim_dbutags/'
AS
WITH LatestUsage AS (
  SELECT
  case when sku_name like '%JOB%' then usage_metadata.job_id
  when sku_name like '%SQL%' then usage_metadata.warehouse_id
  else usage_metadata.cluster_id end as entity_id,
    usage_start_time,
    custom_tags,
    --sku_name,
    usage_metadata,
    ROW_NUMBER() OVER (PARTITION BY case when sku_name like '%JOB%' then usage_metadata.job_id
  when sku_name like '%SQL%' then usage_metadata.warehouse_id
  else usage_metadata.cluster_id end ORDER BY usage_start_time DESC) AS rn
  FROM system.billing.usage
  where custom_tags.CostGroup2 is not null
  --where usage_date < date_format(current_date(),'yyyy-MM-01')
)
SELECT entity_id,upper(replace(custom_tags.Agency,' ','' )) as Agency,
upper(replace(custom_tags.CostGroup1,' ','' )) as CostGroup1,
upper(replace(custom_tags.CostGroup2,' ','' )) as CostGroup2,
upper(ltrim(rtrim(custom_tags.Project))) as Project,
upper(ltrim(rtrim(custom_tags.Client))) as Client
--sku_name
FROM LatestUsage
WHERE rn = 1;

-- COMMAND ----------

-- DBTITLE 1,Create view for most current dbutags 
--Fixing CostGroup2 if any
CREATE OR REPLACE VIEW costusage.source_usage.dim_dbutags_current
AS
WITH LatestUsage AS (
  SELECT
  case when sku_name like '%JOB%' then usage_metadata.job_id
  when sku_name like '%SQL%' then usage_metadata.warehouse_id
  else usage_metadata.cluster_id end as entity_id,
    usage_start_time,
    custom_tags,
    --sku_name,
    usage_metadata,
    ROW_NUMBER() OVER (PARTITION BY case when sku_name like '%JOB%' then usage_metadata.job_id
  when sku_name like '%SQL%' then usage_metadata.warehouse_id
  else usage_metadata.cluster_id end ORDER BY usage_start_time DESC) AS rn
  FROM system.billing.usage
  where custom_tags.CostGroup2 is not null
  -- where usage_date >= date_format(current_date(),'yyyy-MM-01')
)
SELECT entity_id,upper(replace(custom_tags.Agency,' ','' )) as Agency,
upper(replace(custom_tags.CostGroup1,' ','' )) as CostGroup1,
case when upper(replace(custom_tags.Agency,' ','' )) like '%PRECISION%' or upper(replace(custom_tags.CostGroup2,' ','' )) like '%PMX%' OR upper(replace(custom_tags.Agency,' ','' )) like '%PAS%' then 'PAS-US-DATABRICKS'
when upper(replace(custom_tags.CostGroup2,' ','' )) like '%DATALAKE%' then  'DTI-US-DATABRICKS'
when upper(replace(custom_tags.CostGroup2,' ','' )) like '%MAXIMZIER%' or upper(replace(custom_tags.CostGroup2,' ','' )) like '%AUDIENCE%' then 'MAXIMIZER-US-DATABRICKS' 
when upper(replace(custom_tags.CostGroup2,' ','' )) like '%DPR%' then 'DPR-US-DATABRICKS' 
when upper(replace(custom_tags.CostGroup2,' ','' )) like '%LAB%' then 'LAB-US-DATABRICKS' 
when upper(replace(custom_tags.CostGroup2,' ','' )) like '%GROWTH%' then 'GROWTHOS-US-DATABRICKS' 
else upper(replace(custom_tags.CostGroup2,' ','' ))  end as CostGroup2,
upper(ltrim(rtrim(custom_tags.Project))) as Project,
upper(ltrim(rtrim(custom_tags.Client))) as Client
--,sku_name
FROM LatestUsage
WHERE rn = 1;

-- COMMAND ----------

-- DBTITLE 1,Historical billing data - from cost usage dashboard
CREATE TABLE  IF NOT EXISTS costusage.dev_usage.fact_dbrusage_hist_beforeJul
USING csv
OPTIONS (
  'header' 'true',
  'inferSchema' 'true',
  'path' 's3://pm-epsilon-athena/databricks/dim_tables/backup/2022_12-2023_11.csv',
  'mergeSchema' 'true'
);
select * from costusage.dev_usage.fact_dbrusage_hist_beforeJul limit 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### different environment usage
-- MAGIC costusage.${environment}_usage

-- COMMAND ----------

-- DBTITLE 1,Table: fact_dbrusage for archive - Jul 2023 and later
CREATE OR REPLACE TABLE costusage.${environment}_usage.fact_dbrusage
USING DELTA
LOCATION 's3://pm-epsilon-athena/databricks/${environment}/${environment}_usage/fact_dbrusage/'
PARTITIONED BY (year , month)
AS
WITH initalusage AS (
select 
  usage.*,
  sku.sku_listed_rate,
  sku.sku_contract_rate,
  sku.entity_type,
  -- workspace.workspace,
  -- workspace.workspace_url,
  case when usage.sku_name like '%JOB%' then usage_metadata.job_id
  when usage.sku_name like '%SQL%' then usage_metadata.warehouse_id
  else usage_metadata.cluster_id end as entity_id
FROM
  system.billing.usage as usage
left join
  costusage.source_usage.dim_skupricing as sku on usage.sku_name = sku.sku_name
-- left join
--   costusage.source_usage.dim_workspaces as workspace on usage.workspace_id = workspace.workspace_id
where usage_date < date_format(current_date(),'yyyy-MM-01')
and usage_date >= to_date('2023-07-01','yyyy-MM-dd')
)
SELECT
  year(usage_date) as year,
  month(usage_date) as month,
  date_format(usage_date,'yyyy-MM-dd')::date as usagedate,  
  -- workspace,
  workspace_id,
  -- workspace_url,
  tags.Agency as Agency,
  tags.CostGroup1 as CostGroup1,
  CASE 
  When (ltrim(tags.CostGroup2) IS  null and workspace_id = 259119254455886) OR (upper(tags.CostGroup2) LIKE '%AUDIENCE%') OR (upper(tags.CostGroup2) LIKE '%MAXIMZIER%') THEN 'MAXIMIZER-US-DATABRICKS'  
  When (ltrim(tags.CostGroup2) IS  null and workspace_id = 294243493673753) OR  (upper(tags.CostGroup2) LIKE '%PAS%') OR  (upper(tags.CostGroup2) LIKE '%Precision%') OR  (upper(tags.CostGroup2) LIKE '%PMX%') THEN 'PAS-US-DATABRICKS'  
  When (ltrim(tags.CostGroup2) IS  null and workspace_id = 2415015179768953) THEN 'DIGITAS-US-DATABRICKS'  
  When (ltrim(tags.CostGroup2) IS  null and workspace_id = 5119006976089872) OR  (upper(tags.CostGroup2) LIKE '%COLLECTIVE%') THEN 'COLLECTIVE-US-DATABRICKS'
  When (ltrim(tags.CostGroup2) IS  null and workspace_id = 2702753715296702) OR (upper(tags.CostGroup2) LIKE '%LAB%') THEN 'LAB-US-DATABRICKS'
  When (ltrim(tags.CostGroup2) IS  null and workspace_id = 5445138883374470)  OR (upper(tags.CostGroup2) LIKE '%DPR%') THEN 'DPR-US-DATABRICKS'
  When (upper(tags.CostGroup2) LIKE '%GROWTHOS%') THEN 'GROWTHOS-US-DATABRICKS'
  When (upper(tags.CostGroup2) LIKE '%DATALAKE%') THEN 'DTI-US-DATABRICKS' 
  else upper(tags.CostGroup2) end as CostGroup2,
  tags.Project as Project,
  tags.Client as Client,
  usage.sku_name,
  sku_listed_rate,
  sku_contract_rate,
  entity_type,
  usage.entity_id,
  sum(usage_quantity) as usage_quantity,
  sum(usage_quantity)*sku_listed_rate as usage_cost,
  sum(usage_quantity)*sku_contract_rate as usage_cost_discounted
FROM
  initalusage as usage
  left join costusage.source_usage.dim_dbutags as tags
on usage.entity_id = tags.entity_id
-- and usage.sku_name= tags.sku_name
GROUP BY 
  year ,
  month,
  usagedate,
  workspace_id,
  -- workspace,
  -- workspace_url,
  Agency,
  CostGroup1,
  CostGroup2,  
  Project,
  Client,
  usage.sku_name,
  sku_listed_rate,
  sku_contract_rate,
  entity_type,
  usage.entity_id

-- COMMAND ----------

--default data update


-- COMMAND ----------


CREATE DATABASE IF NOT EXISTS costusage.${environment}_usage
MANAGED LOCATION 's3://pm-epsilon-athena/databricks/${environment}/';

-- COMMAND ----------

-- DBTITLE 1,View: v_fact_dbrusage_current

CREATE OR REPLACE VIEW costusage.${environment}_usage.v_fact_dbrusage_current
as
WITH initalusage AS (
select 
  usage.*,
  sku.sku_listed_rate,
  sku.sku_contract_rate,
  sku.entity_type,
  -- workspace.workspace,
  -- workspace.workspace_url,
  case when usage.sku_name like '%JOB%' then usage_metadata.job_id
  when usage.sku_name like '%SQL%' then usage_metadata.warehouse_id
  else usage_metadata.cluster_id end as entity_id
FROM
  system.billing.usage as usage
left join
  costusage.source_usage.dim_skupricing as sku on usage.sku_name = sku.sku_name
-- left join
--   costusage.source_usage.dim_workspaces as workspace on usage.workspace_id = workspace.workspace_id
where usage_date >= date_format(current_date(),'yyyy-MM-01')
)
SELECT
  year(usage_date) as year,
  month(usage_date) as month,
  date_format(usage_date,'yyyy-MM-dd')::date as usagedate,
  -- workspace,
  workspace_id,
  -- workspace_url,
  tags.Agency as Agency,
  tags.CostGroup1 as CostGroup1,
  CASE 
  When (ltrim(tags.CostGroup2) IS  null and workspace_id = 259119254455886) OR (upper(tags.CostGroup2) LIKE '%AUDIENCE%') OR (upper(tags.CostGroup2) LIKE '%MAXIMZIER%') THEN 'MAXIMIZER-US-DATABRICKS'  
  When (ltrim(tags.CostGroup2) IS  null and workspace_id = 294243493673753) OR  (upper(tags.CostGroup2) LIKE '%PAS%') OR  (upper(tags.CostGroup2) LIKE '%Precision%') OR  (upper(tags.CostGroup2) LIKE '%PMX%') THEN 'PAS-US-DATABRICKS'  
  When (ltrim(tags.CostGroup2) IS  null and workspace_id = 2415015179768953) THEN 'DIGITAS-US-DATABRICKS'  
  When (ltrim(tags.CostGroup2) IS  null and workspace_id = 5119006976089872) OR  (upper(tags.CostGroup2) LIKE '%COLLECTIVE%') THEN 'COLLECTIVE-US-DATABRICKS'
  When (ltrim(tags.CostGroup2) IS  null and workspace_id = 2702753715296702) OR (upper(tags.CostGroup2) LIKE '%LAB%') THEN 'LAB-US-DATABRICKS'
  When (ltrim(tags.CostGroup2) IS  null and workspace_id = 5445138883374470)  OR (upper(tags.CostGroup2) LIKE '%DPR%') THEN 'DPR-US-DATABRICKS'
  When (upper(tags.CostGroup2) LIKE '%GROWTHOS%') THEN 'GROWTHOS-US-DATABRICKS'
  When (upper(tags.CostGroup2) LIKE '%DATALAKE%') THEN 'DTI-US-DATABRICKS' 
  else upper(tags.CostGroup2) end as CostGroup2,
  tags.Project as Project,
  tags.Client as Client,
  usage.sku_name,
  sku_listed_rate,
  sku_contract_rate,
  entity_type,
  usage.entity_id,
  sum(usage_quantity) as usage_quantity,
  sum(usage_quantity)*sku_listed_rate as usage_cost,
  sum(usage_quantity)*sku_contract_rate as usage_cost_discounted
FROM
  initalusage as usage
  left join costusage.source_usage.dim_dbutags_current as tags
on usage.entity_id = tags.entity_id
--and usage.sku_name= tags.sku_name
GROUP BY 
  year ,
  month,
  usagedate,
  workspace_id,
  -- workspace,
  -- workspace_url,
  Agency,
  CostGroup1,
  CostGroup2,  
  Project,
  Client,
  usage.sku_name,
  sku_listed_rate,
  sku_contract_rate,
  entity_type,
  usage.entity_id

-- COMMAND ----------

-- MAGIC %md
-- MAGIC v_fact_dbrusage_previous

-- COMMAND ----------

-- DBTITLE 1,View: v_fact_dbrusage_previous- for steaming archive process
CREATE OR REPLACE view costusage.${environment}_usage.v_fact_dbrusage_previous
AS
WITH initalusage AS (
select 
  usage.*,
  sku.sku_listed_rate,
  sku.sku_contract_rate,
  sku.entity_type,
  -- workspace.workspace,
  -- workspace.workspace_url,
  case when usage.sku_name like '%JOB%' then usage_metadata.job_id
  when usage.sku_name like '%SQL%' then usage_metadata.warehouse_id
  else usage_metadata.cluster_id end as entity_id
FROM
  system.billing.usage as usage
left join
  costusage.source_usage.dim_skupricing as sku on usage.sku_name = sku.sku_name
-- left join
--   costusage.source_usage.dim_workspaces as workspace on usage.workspace_id = workspace.workspace_id
where usage_date < date_format(current_date(),'yyyy-MM-01')
and usage_date >= date_format(dateadd(MONTH, -1, current_date()), 'yyyy-MM-01'))
SELECT
  year(usage_date) as year,
  month(usage_date) as month,
  date_format(usage_date,'yyyy-MM-dd')::date as usagedate,
  -- workspace,
  workspace_id,
  -- workspace_url,
  tags.Agency as Agency,
  tags.CostGroup1 as CostGroup1,
  CASE 
  When (ltrim(tags.CostGroup2) IS  null and workspace_id = 259119254455886) OR (upper(tags.CostGroup2) LIKE '%AUDIENCE%') OR (upper(tags.CostGroup2) LIKE '%MAXIMZIER%') THEN 'MAXIMIZER-US-DATABRICKS'  
  When (ltrim(tags.CostGroup2) IS  null and workspace_id = 294243493673753) OR  (upper(tags.CostGroup2) LIKE '%PAS%') OR  (upper(tags.CostGroup2) LIKE '%Precision%') OR  (upper(tags.CostGroup2) LIKE '%PMX%') THEN 'PAS-US-DATABRICKS'   
  When (ltrim(tags.CostGroup2) IS  null and workspace_id = 2415015179768953) THEN 'DIGITAS-US-DATABRICKS'  
  When (ltrim(tags.CostGroup2) IS  null and workspace_id = 5119006976089872) OR  (upper(tags.CostGroup2) LIKE '%COLLECTIVE%') THEN 'COLLECTIVE-US-DATABRICKS'
  When (ltrim(tags.CostGroup2) IS  null and workspace_id = 2702753715296702) OR (upper(tags.CostGroup2) LIKE '%LAB%') THEN 'LAB-US-DATABRICKS'
  When (ltrim(tags.CostGroup2) IS  null and workspace_id = 5445138883374470)  OR (upper(tags.CostGroup2) LIKE '%DPR%') THEN 'DPR-US-DATABRICKS'
  When (upper(tags.CostGroup2) LIKE '%GROWTHOS%') THEN 'GROWTHOS-US-DATABRICKS'
  When (upper(tags.CostGroup2) LIKE '%DATALAKE%') THEN 'DTI-US-DATABRICKS' 
  else upper(tags.CostGroup2) end as CostGroup2,
  tags.Project as Project,
  tags.Client as Client,
  usage.sku_name,
  sku_listed_rate,
  sku_contract_rate,
  entity_type,
  usage.entity_id,
  sum(usage_quantity) as usage_quantity,
  sum(usage_quantity)*sku_listed_rate as usage_cost,
  sum(usage_quantity)*sku_contract_rate as usage_cost_discounted
FROM
  initalusage as usage
  left join costusage.source_usage.dim_dbutags as tags
on usage.entity_id = tags.entity_id
-- and usage.sku_name= tags.sku_name
GROUP BY 
  year ,
  month,
  usagedate,
  workspace_id,
  -- workspace,
  -- workspace_url,
  Agency,
  CostGroup1,
  CostGroup2,  
  Project,
  Client,
  usage.sku_name,
  sku_listed_rate,
  sku_contract_rate,
  entity_type,
  usage.entity_id

-- COMMAND ----------

-- DBTITLE 1,View: v_fact_dbrusage
CREATE OR REPLACE VIEW costusage.${environment}_usage.v_fact_dbrusage
AS
SELECT year,month,usagedate,workspace_id,Agency,CostGroup1,CostGroup2,Project,Client,sku_name,sku_listed_rate,sku_contract_rate,entity_type,entity_id,usage_quantity,usage_cost,usage_cost_discounted
from costusage.${environment}_usage.fact_dbrusage
union all
SELECT year,month,usagedate,workspace_id,Agency,CostGroup1,CostGroup2,Project,Client,sku_name,sku_listed_rate,sku_contract_rate,entity_type,entity_id,usage_quantity,usage_cost,usage_cost_discounted
from costusage.${environment}_usage.v_fact_dbrusage_current

-- COMMAND ----------

-- DBTITLE 1,Silver table: aggr_dbrusage_bycostgroup - Aggregate databricks usage  by costgroup
create or replace table costusage.${environment}_usage.aggr_dbrusage_bycostgroup
AS
with aggr_dbrusage as (
  select
    year,
    month,
    to_date(concat(cast(year as string), '-', lpad(cast(month as string), 2, '0'), '-01'), 'yyyy-MM-dd') as yearmonth,
    case
      when CostGroup2 like '%CMS%' 
      OR CostGroup2 like '%DPR%'
      OR CostGroup2 like '%LAB%'
      OR CostGroup2 like '%GROWTHOS%'
      OR CostGroup2 is null then 'DTI-US-DATABRICKS'
      else CostGroup2
    end as CostGroup,
    sum(usage_cost_discounted) as usage_cost
  from
    costusage.${environment}_usage.fact_dbrusage
  group by
    year,
    month,
    CostGroup
)
select year,month,dbr.CostGroup,dbr.usage_cost
,ifnull(cts.contract_id ,cts_parent.contract_id) AS contract_id
,ifnull(cts.commited_indollars ,0) as commited
,ifnull(cts.`annual budget`,0) as annual_budget
,ifnull(cts.`monthly budget`,0) as month_budget
,case when ifnull(cts.isactive,cts_parent.isactive) = 1 then ifnull(cts.hard_limit,95) 
else ifnull(cts.hard_limit,ifnull(cts.`monthly budget`,cts_parent.`monthly budget`)) end as hard_limit
,ifnull(cts.isactive,cts_parent.isactive) as contract_status 
from  aggr_dbrusage as dbr
left join costusage.source_usage.dim_contracts as cts 
on 
(
  dbr.CostGroup like concat(cts.contract_tag_value, '%') 
)
and cts.parent_contract_id != cts.contract_id
and dbr.yearmonth >=  to_date(cts.datefrom,"yyyy-MM-dd")
and dbr.yearmonth <=  to_date(cts.dateto,"yyyy-MM-dd")
left join costusage.source_usage.dim_contracts as cts_parent 
on 
(
  dbr.CostGroup not like concat(cts_parent.contract_tag_value, '%') 
)
and cts_parent.contract_tag_key = 'CostGroup2' and cts_parent.contract_tag_value = 'DTI'
and dbr.yearmonth >=  to_date(cts_parent.datefrom,"yyyy-MM-dd")
and dbr.yearmonth <=  to_date(cts_parent.dateto,"yyyy-MM-dd")

-- COMMAND ----------

-- DBTITLE 1,View: v_aggr_dbrusage_bycostgroup_current
create or replace view costusage.${environment}_usage.v_aggr_dbrusage_bycostgroup_current
AS
with aggr_dbrusage as (
  select
    year,
    month,
    to_date(concat(cast(year as string), '-', lpad(cast(month as string), 2, '0'), '-01'), 'yyyy-MM-dd') as yearmonth,    
    case
      when CostGroup2 like '%CMS%'
      OR CostGroup2 like '%DPR%'
      OR CostGroup2 like '%LAB%'
      OR CostGroup2 like '%GROWTHOS%'
      OR CostGroup2 is null then 'DTI-US-DATABRICKS'
      else CostGroup2
    end as CostGroup,
    sum(usage_cost_discounted) as usage_cost
  from
    costusage.${environment}_usage.v_fact_dbrusage_current
  group by
    year,
    month,
    CostGroup
)
select year,month,dbr.CostGroup,dbr.usage_cost
,ifnull(cts.contract_id ,cts_parent.contract_id) AS contract_id
,ifnull(cts.commited_indollars ,0) as commited
,ifnull(cts.`annual budget`,0) as annual_budget
,ifnull(cts.`monthly budget`,0) as month_budget
,case when ifnull(cts.isactive,cts_parent.isactive) = 1 then ifnull(cts.hard_limit,95) 
else ifnull(cts.hard_limit,ifnull(cts.`monthly budget`,cts_parent.`monthly budget`)) end as hard_limit
,ifnull(cts.isactive,cts_parent.isactive) as contract_status 
from  aggr_dbrusage as dbr
left join costusage.source_usage.dim_contracts as cts 
on 
(
  dbr.CostGroup like concat(cts.contract_tag_value, '%') 
)
and cts.parent_contract_id != cts.contract_id
and dbr.yearmonth >=  to_date(cts.datefrom,"yyyy-MM-dd")
and dbr.yearmonth <=  to_date(cts.dateto,"yyyy-MM-dd")
left join costusage.source_usage.dim_contracts as cts_parent 
on 
(
  dbr.CostGroup not like concat(cts_parent.contract_tag_value, '%') 
)
and cts_parent.contract_tag_key = 'CostGroup2' and cts_parent.contract_tag_value = 'DTI'
and dbr.yearmonth >=  to_date(cts_parent.datefrom,"yyyy-MM-dd")
and dbr.yearmonth <=  to_date(cts_parent.dateto,"yyyy-MM-dd")

-- COMMAND ----------

-- DBTITLE 1,View: v_aggr_dbrusage_bycostgroup_previous
create or replace view costusage.${environment}_usage.v_aggr_dbrusage_bycostgroup_previous
AS
with aggr_dbrusage as (
  select
    year,
    month,
    to_date(concat(cast(year as string), '-', lpad(cast(month as string), 2, '0'), '-01'), 'yyyy-MM-dd') as yearmonth,
    case
      when CostGroup2 like '%CMS%'
      OR CostGroup2 like '%LAB%'
      OR CostGroup2 like '%DPR%'
      OR CostGroup2 like '%GROWTHOS%'
      OR CostGroup2 is null then 'DTI-US-DATABRICKS'
      else CostGroup2
    end as CostGroup,
    sum(usage_cost_discounted) as usage_cost
  from
    costusage.${environment}_usage.v_fact_dbrusage_previous
  group by
    year,
    month,
    CostGroup
)
select year,month,dbr.CostGroup,dbr.usage_cost
,ifnull(cts.contract_id ,cts_parent.contract_id) AS contract_id
,ifnull(cts.commited_indollars ,0) as commited
,ifnull(cts.`annual budget`,0) as annual_budget
,ifnull(cts.`monthly budget`,0) as month_budget
,case when ifnull(cts.isactive,cts_parent.isactive) = 1 then ifnull(cts.hard_limit,95) 
else ifnull(cts.hard_limit,ifnull(cts.`monthly budget`,cts_parent.`monthly budget`)) end as hard_limit
,ifnull(cts.isactive,cts_parent.isactive) as contract_status 
from  aggr_dbrusage as dbr
left join costusage.source_usage.dim_contracts as cts 
on 
(
  dbr.CostGroup like concat(cts.contract_tag_value, '%') 
)
and cts.parent_contract_id != cts.contract_id
and dbr.yearmonth >=  to_date(cts.datefrom,"yyyy-MM-dd")
and dbr.yearmonth <=  to_date(cts.dateto,"yyyy-MM-dd")
left join costusage.source_usage.dim_contracts as cts_parent 
on 
(
  dbr.CostGroup not like concat(cts_parent.contract_tag_value, '%') 
)
and cts_parent.contract_tag_key = 'CostGroup2' and cts_parent.contract_tag_value = 'DTI'
and dbr.yearmonth >=  to_date(cts_parent.datefrom,"yyyy-MM-dd")
and dbr.yearmonth <=  to_date(cts_parent.dateto,"yyyy-MM-dd")

-- COMMAND ----------

select
  year,
  month,
  to_date(concat(cast(year as string), '-', lpad(cast(month as string), 2, '0'), '-01'), 'yyyy-MM-dd') as yearmonth,
  CostGroup,
  usage_cost,
  commited,
  annual_budget,
  month_budget,
  hard_limit,
  SUM(usage_cost) OVER (
    PARTITION BY contract_id,
    CostGroup
    ORDER BY
      year,
      month
  ) as cumulative_usage_cost,
    SUM(month_budget-usage_cost) OVER (
    PARTITION BY contract_id,
    CostGroup,
    contract_status
    ORDER BY
      year,
      month
  ) as balance,
  contract_status
from  
(
  select * from costusage.${environment}_usage.aggr_dbrusage_bycostgroup
  union 
  select * from costusage.${environment}_usage.v_aggr_dbrusage_bycostgroup_current
)



-- COMMAND ----------

-- DBTITLE 1,View: v_dbrusage_cumulative
CREATE OR REPLACE view costusage.${environment}_usage.v_dbrusage_cumulative
as 
select
  year,
  month,
  to_date(concat(cast(year as string), '-', lpad(cast(month as string), 2, '0'), '-01'), 'yyyy-MM-dd') as yearmonth,
  CostGroup,
  usage_cost,
  commited,
  annual_budget,
  month_budget,
  hard_limit,
  SUM(usage_cost) OVER (
    PARTITION BY contract_id,
    CostGroup
    ORDER BY
      year,
      month
  ) as cumulative_usage_cost,
    SUM(month_budget-usage_cost) OVER (
    PARTITION BY contract_id,
    CostGroup,
    contract_status
    ORDER BY
      year,
      month
  ) as balance,
  contract_status
from  
(
  select * from costusage.${environment}_usage.aggr_dbrusage_bycostgroup
  union 
  select * from costusage.${environment}_usage.v_aggr_dbrusage_bycostgroup_current
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Sub CostGroup for Agency Dashboard

-- COMMAND ----------

-- DBTITLE 1,Table: aggr_dbrusage_bysubcostgroup
create or replace table costusage.${environment}_usage.aggr_dbrusage_bysubcostgroup
AS
with aggr_dbrusage as (
  select
    year,
    month,
    to_date(concat(cast(year as string), '-', lpad(cast(month as string), 2, '0'), '-01'), 'yyyy-MM-dd') as yearmonth,
    case
      when CostGroup2 like '%CMS%' 
      OR CostGroup2 like '%DPR%'
      OR CostGroup2 like '%LAB%'
      OR CostGroup2 like '%GROWTHOS%'
      OR CostGroup2 is null then 'DTI-US-DATABRICKS'
      else CostGroup2
    end as CostGroup,
    CostGroup2 as sub_costgroup,
    sum(usage_cost_discounted) as usage_cost
  from
    costusage.${environment}_usage.fact_dbrusage
  group by
    year,
    month,
    CostGroup,
    sub_costgroup
)
select year,month,dbr.CostGroup as costgroup, dbr.sub_costgroup ,dbr.usage_cost
,ifnull(cts.contract_id ,cts_parent.contract_id) AS contract_id
,ifnull(cts.commited_indollars ,0) as commited
,ifnull(cts.`annual budget`,0) as annual_budget
,ifnull(cts.`monthly budget`,0) as month_budget
,case when ifnull(cts.isactive,cts_parent.isactive) = 1 then ifnull(cts.hard_limit,95) 
else ifnull(cts.hard_limit,ifnull(cts.`monthly budget`,cts_parent.`monthly budget`)) end as hard_limit
,ifnull(cts.isactive,cts_parent.isactive) as contract_status 
from  aggr_dbrusage as dbr
left join costusage.source_usage.dim_contracts as cts 
on 
(
  dbr.CostGroup like concat(cts.contract_tag_value, '%') 
)
and cts.parent_contract_id != cts.contract_id
and dbr.yearmonth >=  to_date(cts.datefrom,"yyyy-MM-dd")
and dbr.yearmonth <=  to_date(cts.dateto,"yyyy-MM-dd")
left join costusage.source_usage.dim_contracts as cts_parent 
on 
(
  dbr.CostGroup not like concat(cts_parent.contract_tag_value, '%') 
)
and cts_parent.contract_tag_key = 'CostGroup2' and cts_parent.contract_tag_value = 'DTI'
and dbr.yearmonth >=  to_date(cts_parent.datefrom,"yyyy-MM-dd")
and dbr.yearmonth <=  to_date(cts_parent.dateto,"yyyy-MM-dd")

-- COMMAND ----------

-- DBTITLE 1,View: v_aggr_dbrusage_bysubcostgroup_current
create or replace view costusage.${environment}_usage.v_aggr_dbrusage_bysubcostgroup_current
AS
with aggr_dbrusage as (
  select
    year,
    month,
    to_date(concat(cast(year as string), '-', lpad(cast(month as string), 2, '0'), '-01'), 'yyyy-MM-dd') as yearmonth,    
    case
      when CostGroup2 like '%CMS%'
      OR CostGroup2 like '%DPR%'
      OR CostGroup2 like '%LAB%'
      OR CostGroup2 like '%GROWTHOS%'
      OR CostGroup2 is null then 'DTI-US-DATABRICKS'
      else CostGroup2
    end as CostGroup,
    CostGroup2 as sub_costgroup,
    sum(usage_cost_discounted) as usage_cost
  from
    costusage.${environment}_usage.v_fact_dbrusage_current
  group by
    year,
    month,
    CostGroup,
    sub_costgroup
)
select year,month,dbr.CostGroup as costgroup,dbr.sub_costgroup,dbr.usage_cost
,ifnull(cts.contract_id ,cts_parent.contract_id) AS contract_id
,ifnull(cts.commited_indollars ,0) as commited
,ifnull(cts.`annual budget`,0) as annual_budget
,ifnull(cts.`monthly budget`,0) as month_budget
,case when ifnull(cts.isactive,cts_parent.isactive) = 1 then ifnull(cts.hard_limit,95) 
else ifnull(cts.hard_limit,ifnull(cts.`monthly budget`,cts_parent.`monthly budget`)) end as hard_limit
,ifnull(cts.isactive,cts_parent.isactive) as contract_status 
from  aggr_dbrusage as dbr
left join costusage.source_usage.dim_contracts as cts 
on 
(
  dbr.CostGroup like concat(cts.contract_tag_value, '%') 
)
and cts.parent_contract_id != cts.contract_id
and dbr.yearmonth >=  to_date(cts.datefrom,"yyyy-MM-dd")
and dbr.yearmonth <=  to_date(cts.dateto,"yyyy-MM-dd")
left join costusage.source_usage.dim_contracts as cts_parent 
on 
(
  dbr.CostGroup not like concat(cts_parent.contract_tag_value, '%') 
)
and cts_parent.contract_tag_key = 'CostGroup2' and cts_parent.contract_tag_value = 'DTI'
and dbr.yearmonth >=  to_date(cts_parent.datefrom,"yyyy-MM-dd")
and dbr.yearmonth <=  to_date(cts_parent.dateto,"yyyy-MM-dd")

-- COMMAND ----------

-- DBTITLE 1,View v_aggr_dbrusage_bysubcostgroup_previous
create or replace view costusage.${environment}_usage.v_aggr_dbrusage_bysubcostgroup_previous
AS
with aggr_dbrusage as (
  select
    year,
    month,
    to_date(concat(cast(year as string), '-', lpad(cast(month as string), 2, '0'), '-01'), 'yyyy-MM-dd') as yearmonth,
    case
      when CostGroup2 like '%CMS%'
      OR CostGroup2 like '%LAB%'
      OR CostGroup2 like '%DPR%'
      OR CostGroup2 like '%GROWTHOS%'
      OR CostGroup2 is null then 'DTI-US-DATABRICKS'
      else CostGroup2
    end as CostGroup,
    CostGroup2 as sub_costgroup,
    sum(usage_cost_discounted) as usage_cost
  from
    costusage.${environment}_usage.v_fact_dbrusage_previous
  group by
    year,
    month,
    CostGroup,
    sub_costgroup
)
select year,month,dbr.CostGroup as costgroup,dbr.sub_costgroup,dbr.usage_cost
,ifnull(cts.contract_id ,cts_parent.contract_id) AS contract_id
,ifnull(cts.commited_indollars ,0) as commited
,ifnull(cts.`annual budget`,0) as annual_budget
,ifnull(cts.`monthly budget`,0) as month_budget
,case when ifnull(cts.isactive,cts_parent.isactive) = 1 then ifnull(cts.hard_limit,95) 
else ifnull(cts.hard_limit,ifnull(cts.`monthly budget`,cts_parent.`monthly budget`)) end as hard_limit
,ifnull(cts.isactive,cts_parent.isactive) as contract_status 
from  aggr_dbrusage as dbr
left join costusage.source_usage.dim_contracts as cts 
on 
(
  dbr.CostGroup like concat(cts.contract_tag_value, '%') 
)
and cts.parent_contract_id != cts.contract_id
and dbr.yearmonth >=  to_date(cts.datefrom,"yyyy-MM-dd")
and dbr.yearmonth <=  to_date(cts.dateto,"yyyy-MM-dd")
left join costusage.source_usage.dim_contracts as cts_parent 
on 
(
  dbr.CostGroup not like concat(cts_parent.contract_tag_value, '%') 
)
and cts_parent.contract_tag_key = 'CostGroup2' and cts_parent.contract_tag_value = 'DTI'
and dbr.yearmonth >=  to_date(cts_parent.datefrom,"yyyy-MM-dd")
and dbr.yearmonth <=  to_date(cts_parent.dateto,"yyyy-MM-dd")

-- COMMAND ----------

-- DBTITLE 1,View: v_dbrusage_cumulative_subcostgroup
CREATE OR REPLACE view costusage.${environment}_usage.v_dbrusage_cumulative_subcostgroup
as 
select
  year,
  month,
  to_date(concat(cast(year as string), '-', lpad(cast(month as string), 2, '0'), '-01'), 'yyyy-MM-dd') as yearmonth,
  costgroup,
  sub_costgroup,
  usage_cost,
  case when costgroup = sub_costgroup then commited else 0 end commited,
  case when costgroup = sub_costgroup then annual_budget else 0 end annual_budget,
  case when costgroup = sub_costgroup then month_budget else 0 end month_budget,
  case when costgroup = sub_costgroup then hard_limit else 0 end hard_limit,
  SUM(usage_cost) OVER (
    PARTITION BY contract_id,
    costgroup,
    sub_costgroup
    ORDER BY
      year,
      month
  ) as cumulative_usage_cost,
  case when costgroup = sub_costgroup then SUM(usage_cost) OVER (
    PARTITION BY contract_id,
    costgroup
    ORDER BY
      year,
      month
  ) else 0 end as cumulative_contract_usage_cost,
  case when costgroup = sub_costgroup then SUM(case when costgroup = sub_costgroup then month_budget ELSE 0 END) OVER (
    PARTITION BY contract_id,
    costgroup
    ORDER BY
      year,
      month
  ) else 0 end AS cumulative_month_budget,
  cumulative_month_budget - cumulative_contract_usage_cost  as cumulative_month_balance,
  contract_status
from  
(
  select * from costusage.${environment}_usage.aggr_dbrusage_bysubcostgroup
  union 
  select * from costusage.${environment}_usage.v_aggr_dbrusage_bysubcostgroup_current
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Auditing
-- MAGIC
-- MAGIC https://pm-di-us.cloud.databricks.com/?o=3339421385882115#notebook/1105447531682789/command/1392052969649612

-- COMMAND ----------

-- DBTITLE 1,expect no results
SELECT entity_id,count(1) from costusage.source_usage.dim_dbutags_current group by entity_id
having  count(1) > 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC AWS Cost Usage

-- COMMAND ----------

-- DBTITLE 1,Create external aws usage tables 


-- COMMAND ----------

SHOW TABLES IN costusage.${environment}_usage
