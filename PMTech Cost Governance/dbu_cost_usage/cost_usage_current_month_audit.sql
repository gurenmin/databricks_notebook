-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Source_Usage Catalog

-- COMMAND ----------

REFRESH TABLE costusage.source_usage.dim_customtags;
REFRESH TABLE costusage.source_usage.dim_contracts;
REFRESH TABLE costusage.source_usage.dim_workspaces;
REFRESH TABLE costusage.source_usage.dim_skupricing;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### expect no result for duplicate entity_id

-- COMMAND ----------

SELECT entity_id,count(1) from costusage.source_usage.dim_dbutags_current group by entity_id
having  count(1) > 1

-- COMMAND ----------

-- DBTITLE 1,check tag
SELECT distinct max(year*100+month) as yearmonth,ws.workspace,CostGroup2,sku_name,entity_type,entity_id,project,
case when entity_type = 'JOB' THEN concat('https://',ws.workspace_url,'.cloud.databricks.com/?o=',usage.workspace_id,'#job/',entity_id) 
when entity_type like 'COMPUTE%' THEN concat('https://',ws.workspace_url,'.cloud.databricks.com/?o=',usage.workspace_id,'#setting/clusters/',entity_id) 
when entity_type like 'SQL%' THEN concat('https://',ws.workspace_url,'.cloud.databricks.com/sql/warehouses/',entity_id,'?o=',usage.workspace_id) 
else concat('https://',ws.workspace_url,'.cloud.databricks.com/?o=',usage.workspace_id,'#setting/clusters/',entity_id)  end as link,
sum(usage_cost_discounted) usage_cost_discounted
from costusage.${environment}_usage.v_fact_dbrusage as usage
left join costusage.source_usage.dim_workspaces as ws on usage.workspace_id=ws.workspace_id
where (CostGroup2 is null OR CostGroup2 NOT LIKE '%US-DATABRICKS%' or workspace is null or entity_type is null) and entity_id is not null
group by ws.workspace,sku_name,entity_type,entity_id,usage.workspace_id,CostGroup2,ws.workspace_url,project
--having max(year*100+month) = year(current_date())*100 +month(current_date())
order by yearmonth desc,usage_cost_discounted desc

-- COMMAND ----------

-- DBTITLE 1,Check the aggr data if any CostGroup2 need to be fixed
SELECT distinct max(year*100+month) as yearmonth,ws.workspace,CostGroup2,sku_name,entity_type,entity_id,project,
case when entity_type = 'JOB' THEN concat('https://',ws.workspace_url,'.cloud.databricks.com/?o=',usage.workspace_id,'#job/',entity_id) 
when entity_type like 'COMPUTE%' THEN concat('https://',ws.workspace_url,'.cloud.databricks.com/?o=',usage.workspace_id,'#setting/clusters/',entity_id) 
when entity_type like 'SQL%' THEN concat('https://',ws.workspace_url,'.cloud.databricks.com/sql/warehouses/',entity_id,'?o=',usage.workspace_id) 
else concat('https://',ws.workspace_url,'.cloud.databricks.com/?o=',usage.workspace_id,'#setting/clusters/',entity_id)  end as link,
sum(usage_cost_discounted) usage_cost_discounted
from costusage.${environment}_usage.v_fact_dbrusage as usage
left join costusage.source_usage.dim_workspaces as ws on usage.workspace_id=ws.workspace_id
where (CostGroup2 is null OR CostGroup2 NOT LIKE '%US-DATABRICKS%' or workspace is null or entity_type is null) and entity_id is not null
group by ws.workspace,sku_name,entity_type,entity_id,usage.workspace_id,CostGroup2,ws.workspace_url,project
--having max(year*100+month) = year(current_date())*100 +month(current_date())
order by yearmonth desc,usage_cost_discounted desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Clean Up Historical Untagged Assets

-- COMMAND ----------

-- DBTITLE 1,Update the Missing Tag data
update costusage.${environment}_usage.fact_dbrusage set CostGroup2 = 'PAS-US-DATABRICKS', Agency ='PMX'
WHERE CostGroup2 = 'PMX';

update costusage.${environment}_usage.fact_dbrusage set CostGroup2 = 'DTI-US-DATABRICKS'
where workspace_id ='7509956532948377' and CostGroup2 is NULL; 

update costusage.${environment}_usage.fact_dbrusage set CostGroup2 = 'PAS-US-DATABRICKS'
where entity_id IN('5f557fc442276d4b','035ac6f21ccc1964','992760803193755','755918010195069','624011171531522','1066130183161618','653911635989676','319299244964279') and CostGroup2 is NULL;

update costusage.${environment}_usage.fact_dbrusage set CostGroup2 = 'DTI-US-DATABRICKS'
where entity_id IN ('10a4194063759e3e','1017-132528-asb7xypf','4280ddc76cc9c8c5','0205-214648-ho38ymfl','0828-200031-x41luawz','420ff315adb2ac4b','211309913079754','0725-000039-24m8il3s','1205-215642-8pec9xx3','419b5d44cdd383fe','0904-173807-5d2ttobg','7018311965ab23a9','0904-183030-vd11d530','440345718949297','0807-192658-q147sub6','0830-175939-wlh7us8u','e46f166c2d7b1f8f','0725-004916-m0ibwzzo','402b96c44dbbc909','727020076788137','0727-195903-jwjevzeh','0725-004916-m0ibwzzo','60f6a728df553076','682a33ca7925dbea','0207-193402-9stz71qw','80b8b0590afa530d','80b8b0590afa530d') and CostGroup2 is NULL;

update costusage.${environment}_usage.fact_dbrusage set CostGroup2 = 'CMS-US-DATABRICKS'
where entity_id IN ('716544031961910','143586412148325') and CostGroup2 is NULL;


update costusage.${environment}_usage.fact_dbrusage set CostGroup1 = 'DATABRICKS-US'
where CostGroup1 is NULL;

-- COMMAND ----------

--current month
select * from costusage.${environment}_usage.v_fact_dbrusage_current where costgroup2 is null

-- COMMAND ----------

select * from costusage.${environment}_usage.aggr_dbrusage_bysubcostgroup 

-- COMMAND ----------

select * from costusage.${environment}_usage.fact_dbrusage where CostGroup2 is null

-- COMMAND ----------

-- DBTITLE 1,update historical data
UPDATE costusage.${environment}_usage.fact_dbrusage set CostGroup2 ='DPR-US-DATABRICKS', Project ='DPR' where CostGroup2 ='DPR';
UPDATE costusage.${environment}_usage.aggr_dbrusage_bycostgroup set CostGroup = "DPR-US-DATABRICKS" WHERE CostGroup = "DPR";
UPDATE costusage.${environment}_usage.fact_dbrusage set CostGroup2 ='PAS-US-DATABRICKS', Project ='PMX' where CostGroup2 ='PMX';
UPDATE costusage.${environment}_usage.aggr_dbrusage_bycostgroup set CostGroup = "PAS-US-DATABRICKS" WHERE CostGroup = "PMX";
UPDATE costusage.${environment}_usage.aggr_dbrusage_bysubcostgroup set costgroup = "PAS-US-DATABRICKS", sub_costGroup = "PAS-US-DATABRICKS" WHERE CostGroup = "PMX";

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## IF CostGroup2 Need to be keep but re-categorize to different contract

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # v_fact_dbrusage_previous

-- COMMAND ----------

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
  --update mapping start
  CASE 
  When (ltrim(tags.CostGroup2) IS  null and workspace_id = 259119254455886) OR (upper(tags.CostGroup2) LIKE '%AUDIENCE%') OR (upper(tags.CostGroup2) LIKE '%MAXIMZIER%') THEN 'MAXIMIZER-US-DATABRICKS'  
  When (ltrim(tags.CostGroup2) IS  null and workspace_id = 294243493673753) THEN 'PAS-US-DATABRICKS'  
  When (ltrim(tags.CostGroup2) IS  null and workspace_id = 2415015179768953) THEN 'DIGITAS-US-DATABRICKS'  
  When (ltrim(tags.CostGroup2) IS  null and workspace_id = 5119006976089872) OR  (upper(tags.CostGroup2) LIKE '%COLLECTIVE%') THEN 'COLLECTIVE-US-DATABRICKS'
  When (ltrim(tags.CostGroup2) IS  null and workspace_id = 2702753715296702) OR (upper(tags.CostGroup2) LIKE '%LAB%') THEN 'LAB-US-DATABRICKS'
  When (ltrim(tags.CostGroup2) IS  null and workspace_id = 5445138883374470)  OR (upper(tags.CostGroup2) LIKE '%DPR%') THEN 'DPR-US-DATABRICKS'
  When (upper(tags.CostGroup2) LIKE '%GROWTHOS%' OR upper(tags.CostGroup2) LIKE 'GOS') THEN 'GROWTHOS-US-DATABRICKS'
  When (upper(tags.CostGroup2) LIKE '%DATALAKE%' OR (ltrim(tags.CostGroup2) IS  null and workspace_id = 7509956532948377)) THEN 'DTI-US-DATABRICKS' 
  else upper(tags.CostGroup2) end as CostGroup2,
  --update mapping end
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

-- MAGIC %md
-- MAGIC # v_fact_dbrusage_current

-- COMMAND ----------


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
  When (ltrim(tags.CostGroup2) IS  null and workspace_id = 294243493673753) THEN 'PAS-US-DATABRICKS'  
  When (ltrim(tags.CostGroup2) IS  null and workspace_id = 2415015179768953) THEN 'DIGITAS-US-DATABRICKS'  
  When (ltrim(tags.CostGroup2) IS  null and workspace_id = 5119006976089872) OR  (upper(tags.CostGroup2) LIKE '%COLLECTIVE%') THEN 'COLLECTIVE-US-DATABRICKS'
  When (ltrim(tags.CostGroup2) IS  null and workspace_id = 2702753715296702) OR (upper(tags.CostGroup2) LIKE '%LAB%') THEN 'LAB-US-DATABRICKS'
  When (ltrim(tags.CostGroup2) IS  null and workspace_id = 5445138883374470)  OR (upper(tags.CostGroup2) LIKE '%DPR%') THEN 'DPR-US-DATABRICKS'
  When (upper(tags.CostGroup2) LIKE '%GROWTHOS%' OR upper(tags.CostGroup2) LIKE 'GOS') THEN 'GROWTHOS-US-DATABRICKS'
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

-- DBTITLE 1,fixing aggr_dbvrusage_bycostgroup
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
and dbr.yearmonth <=  to_date(cts_parent.dateto,"yyyy-MM-dd");

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

-- DBTITLE 1,Missing Tag
select ws.workspace,usage.workspace_id,entity_type,entity_id,entity_type,sku_name,
case when entity_type = 'JOB' THEN concat('https://',ws.workspace_url,'.cloud.databricks.com/?o=',usage.workspace_id,'#job/',entity_id) 
when entity_type like 'COMPUTE%' THEN concat('https://',ws.workspace_url,'.cloud.databricks.com/?o=',usage.workspace_id,'#setting/clusters/',entity_id) 
when entity_type like 'SQL%' THEN concat('https://',ws.workspace_url,'.cloud.databricks.com/sql/warehouses/',entity_id,'?o=',usage.workspace_id) 
else concat('https://',ws.workspace_url,'.cloud.databricks.com/?o=',usage.workspace_id,'#setting/clusters/',entity_id)  end as link,
sum(usage_cost_discounted) as usage_cost_discounted
from costusage.${environment}_usage.v_fact_dbrusage as usage
left join costusage.source_usage.dim_workspaces as ws on usage.workspace_id=ws.workspace_id
where CostGroup2 NOT LIKE '%US-DATABRICKS%' or CostGroup2 is null or entity_type is null or usage_cost_discounted is null
group by ws.workspace,usage.workspace_id,ws.workspace_url,entity_type,entity_id,sku_name

-- COMMAND ----------

-- DBTITLE 1,Missing SKU
select * from costusage.${environment}_usage.fact_dbrusage where entity_type is null or  entity_id is null

-- COMMAND ----------

-- DBTITLE 1,Update Missing Sku Pricing
UPDATE costusage.${environment}_usage.fact_dbrusage
SET sku_listed_rate = (
    SELECT max(sku_listed_rate)
    FROM costusage.source_usage.dim_skupricing 
    WHERE sku_name = fact_dbrusage.sku_name
) where sku_listed_rate is null;

UPDATE costusage.${environment}_usage.fact_dbrusage
SET sku_contract_rate = (
    SELECT max(sku_contract_rate)
    FROM costusage.source_usage.dim_skupricing 
    WHERE sku_name = fact_dbrusage.sku_name
) where sku_contract_rate is null;

UPDATE costusage.${environment}_usage.fact_dbrusage
SET entity_type = (
    SELECT max(entity_type)
    FROM costusage.source_usage.dim_skupricing 
    WHERE sku_name = fact_dbrusage.sku_name
) where entity_type is null;

UPDATE costusage.${environment}_usage.fact_dbrusage
SET usage_cost = usage_quantity*(
    SELECT max(sku_listed_rate)
    FROM costusage.source_usage.dim_skupricing 
    WHERE sku_name = fact_dbrusage.sku_name
) where usage_cost is null;

UPDATE costusage.${environment}_usage.fact_dbrusage
SET usage_cost_discounted = usage_quantity*(
    SELECT max(sku_contract_rate)
    FROM costusage.source_usage.dim_skupricing 
    WHERE sku_name = fact_dbrusage.sku_name
) where usage_cost_discounted is null;

-- COMMAND ----------

select * from costusage.${environment}_usage.v_dbrusage_cumulative 
where year = year(current_date()) and month = month(current_date())

-- COMMAND ----------

-- DBTITLE 1,unexpected data auditing
--select distinct CostGroup from costusage.prod_usage.v_aggr_dbrusage_bycostgroup_current

select year,month,usagedate,workspace,Agency,CostGroup1,CostGroup2,Project,Client,sku_name,entity_id,usage_cost from costusage.${environment}_usage.v_fact_dbrusage_current --where CostGroup2 = "PMX"

-- COMMAND ----------

-- DBTITLE 1,dashboard query to use
select
  current_date() UpdatedBy,
  CostGroup,
  year,month,
  sum(usage_cost) month_to_date_cost,
  sum(month_budget) month_budget,
  sum(annual_budget) annual_budget,
  sum(commited) commited_cost,
  case when sum(hard_limit) = 0 then sum(commited)*0.95
  else sum(hard_limit) end as hard_limit,
  sum(cumulative_usage_cost) cumulative_usage_cost,
  case when sum(usage_cost) > sum(month_budget)*0.95 then 10
  when sum(cumulative_usage_cost) > sum(hard_limit) then 100
  else 0 end as alert  
from
costusage.${environment}_usage.v_dbrusage_cumulative
group by CostGroup,year,month
order by year desc, month desc
