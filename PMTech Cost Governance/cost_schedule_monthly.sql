-- Databricks notebook source
-- MAGIC %python
-- MAGIC # dbutils.widgets.help("dropdown")
-- MAGIC # dbutils.widgets.removeAll()
-- MAGIC
-- MAGIC # dbutils.widgets.dropdown("environment", "prod", ["dev", "test", "prod"])

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Source Catalog

-- COMMAND ----------

-- DBTITLE 1,refreshing data from s3
REFRESH TABLE costusage.source_usage.dim_customtags;
REFRESH TABLE costusage.source_usage.dim_contracts;
REFRESH TABLE costusage.source_usage.dim_workspaces;
REFRESH TABLE costusage.source_usage.dim_skupricing;

-- COMMAND ----------

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

-- DBTITLE 1,Refresh table dim_dbutags for with previous(no longer existed)& most current tags

CREATE OR REPLACE TABLE costusage.source_usage.dim_dbutags_previous 
USING delta
LOCATION 's3://pm-epsilon-athena/databricks/dim_tables/dim_dbutags_previous/'
AS
select * from costusage.source_usage.dim_dbutags
WHERE entity_id not in (
  select entity_id from costusage.source_usage.dim_dbutags_current
);

CREATE OR REPLACE TEMPORARY VIEW v_dbutags_previous
AS
select * from costusage.source_usage.dim_dbutags
WHERE entity_id not in (
  select entity_id from costusage.source_usage.dim_dbutags_current
);

CREATE OR REPLACE TABLE costusage.source_usage.dim_dbutags
USING delta
LOCATION 's3://pm-epsilon-athena/databricks/dim_tables/dim_dbutags/'
AS
select * from costusage.source_usage.dim_dbutags_previous
union 
select * from costusage.source_usage.dim_dbutags_current;

--update costusage.source_usage.dim_dbutags set Agency='PMX' where CostGroup2 like 'PAS%' 
--need latest tagging that not in the billing.usage yet

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Environment Run

-- COMMAND ----------

-- DBTITLE 1,Archive previous month data to historical archived tables
insert into costusage.${environment}_usage.fact_dbrusage(year,month,usagedate,workspace_id,Agency,CostGroup1,CostGroup2,Project,Client,sku_name,sku_listed_rate,sku_contract_rate,entity_type,entity_id,usage_quantity,usage_cost,usage_cost_discounted)
select year,month,usagedate,workspace_id,Agency,CostGroup1,CostGroup2,Project,Client,sku_name,sku_listed_rate,sku_contract_rate,entity_type,entity_id,usage_quantity,usage_cost,usage_cost_discounted from costusage.${environment}_usage.v_fact_dbrusage_previous
where year*100+month not in (select year*100+month from  costusage.${environment}_usage.fact_dbrusage);

insert into costusage.${environment}_usage.aggr_dbrusage_bycostgroup(year,month,CostGroup,usage_cost,contract_id,commited,annual_budget,month_budget,hard_limit,contract_status)
select year,month,CostGroup,usage_cost,contract_id,commited,annual_budget,month_budget,hard_limit,contract_status from costusage.${environment}_usage.v_aggr_dbrusage_bycostgroup_previous
where year*100+month not in (select year*100+month from  costusage.${environment}_usage.aggr_dbrusage_bycostgroup);


-- COMMAND ----------

-- DBTITLE 1,Clean Up Process
-- VACUUM costusage.${environment}_usage.aggr_dbrusage_bycostgroup DRY RUN ;
-- VACUUM costusage.source_usage.dim_dbutags DRY RUN
-- VACUUM costusage.${environment}_usage.fact_dbrusage DRY RUN
-- OPTIMIZE aggr_dbrusage_bycostgroup
-- VACUUM costusage.${environment}_usage.fact_dbrusage [RETAIN 168 HOURS] [DRY RUN]|[PURGE]
-- VACUUM costusage.source_usage.dim_dbutags_previous RETAIN 168 HOURS ;
-- VACUUM costusage.source_usage.dim_dbutags RETAIN 168 HOURS DRY RUN
-- ALTER TABLE costusage.${environment}_usage.aggr_dbrusage_bycostgroup SET TBLPROPERTIES (delta.logRetentionDuration = "interval 7 days");
--DESCRIBE HISTORY costusage.${environment}_usage.aggr_dbrusage_bycostgroup
