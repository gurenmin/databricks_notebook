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

case
when entity_id like 'pipelines%' or entity_id like 'sql/warehouses%' or entity_id like 'compute/vector%' or entity_id like 'ml/endpoints%' or  entity_id  like '%job%' or entity_id like '%cluster%'
THEN concat('https://',ws.workspace_url,'.cloud.databricks.com/',entity_id,'?o=',usage.workspace_id) 
when entity_id like '%notebook%' THEN concat('https://',ws.workspace_url,'.cloud.databricks.com/?o=',usage.workspace_id,entity_id)
when entity_type = 'REAL_TIME_INFERENCE SERVERLESS' and entity_id is not null THEN concat('https://',ws.workspace_url,'.cloud.databricks.com/compute/ventor_search/',entity_id,'?o=',usage.workspace_id) 
when entity_type like '%WAREHOUSE%' THEN concat('https://',ws.workspace_url,'.cloud.databricks.com/sql/warehouses/',entity_id,'?o=',usage.workspace_id) 
when entity_type = 'JOB' and entity_id not like '%job%' THEN concat('https://',ws.workspace_url,'.cloud.databricks.com/jobs/',entity_id,'?o=',usage.workspace_id) 
else concat('https://',ws.workspace_url,'.cloud.databricks.com/compute/clusters/',entity_id,'?o=',usage.workspace_id)  end as link,

sum(usage_cost_discounted) usage_cost_discounted
from costusage.${environment}_usage.fact_dbrusage as usage
left join costusage.source_usage.dim_workspaces as ws on usage.workspace_id=ws.workspace_id
where (CostGroup2 is null OR CostGroup2 NOT LIKE '%US-DATABRICKS%' or workspace is null or entity_type is null) --and entity_id is not null
group by ws.workspace,sku_name,entity_type,entity_id,usage.workspace_id,CostGroup2,ws.workspace_url,project
--having max(year*100+month) = year(current_date())*100 +month(current_date())
order by yearmonth desc,usage_cost_discounted desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Clean Up Historical Untagged Assets

-- COMMAND ----------

update costusage.${environment}_usage.fact_dbrusage set  CostGroup1 = 'DATABRICKS-US',CostGroup2 = 'DTI-US-DATABRICKS', Agency ='DTI'
where entity_id IN('compute/vector-search/vector-search-demo-endpoint','compute/vector-search/test_brief_endpoint','compute/vector-search/test_briefs_endpoint','compute/clusters/0112-170613-wq6sctxg','compute/clusters/0328-140330-c0355dg5','jobs/103898729470132','compute/clusters/0508-180925-vfjo0jyj','compute/clusters/0408-151606-8qnbozcy','compute/clusters/0308-190556-4ctsuiuh','compute/clusters/0512-180043-2br8mp9d','compute/clusters/0506-135347-6f7nu1s','compute/clusters/0302-162124-3tonkssh','compute/clusters/0325-135442-4sexw62d','jobs/1060879989389256','jobs/179785376153418','compute/clusters/0131-214851-ld4rsxv9','jobs/947797074375169','jobs/990031306117535','jobs/514650731549945','jobs/38954163915483','compute/clusters/0326-161013-5swqf2pl','jobs/244865951581185','jobs/705264854694377','jobs/358614874734949','jobs/85304385831480','compute/clusters/0506-135347-6f7nu1s1','jobs/507602322602250','jobs/991719229836706','jobs/260321256170389','jobs/659160384728597','jobs/392305391273751') and CostGroup2 is NULL; 
update costusage.${environment}_usage.fact_dbrusage set  CostGroup1 = 'DATABRICKS-US',CostGroup2 = 'PAS-US-DATABRICKS', Agency ='PMX'
where entity_id like '%fdad88a1-3fad-489e-bb30-2a548bec3461'
or entity_id in('jobs/319299244964279','jobs/225068839607863','jobs/421086402362351','jobs/535964478789572','jobs/189446756530471','jobs/423171645528900','jobs/491239544707586','jobs/131568715132866','jobs/740588666973468','jobs/882247704898967','jobs/39512403186443','jobs/997615897566422','jobs/402181099492590','jobs/662192930665737','jobs/226970914059696','jobs/239645347448950','jobs/615337332430672','jobs/416518544878121','jobs/18612505372114','jobs/410540409793420','compute/clusters/0311-161025-5kmfs88b','jobs/858862069306772','jobs/915694970603697','jobs/446541587027532','jobs/1055711113155033','jobs/188705887503938') and CostGroup2 is NULL; 

update costusage.${environment}_usage.fact_dbrusage set  CostGroup1 = 'DATABRICKS-US',CostGroup2 = 'CMS-US-DATABRICKS', Agency ='DTI'
where entity_id IN('compute/clusters/1007-073516-ziz3hxfr','compute/clusters/0710-170817-uhbwjoe1') and CostGroup2 is NULL; 




update costusage.${environment}_usage.fact_dbrusage set  CostGroup1 = 'DATABRICKS-US',CostGroup2 = 'DPR-US-DATABRICKS', Agency ='DTI'
where entity_id IN('compute/clusters/0531-145419-i5sgq8z7') and CostGroup2 is NULL; 

select * from costusage.${environment}_usage.fact_dbrusage where sku_name ='ENTERPRISE_ALL_PURPOSE_SERVERLESS_COMPUTE_US_EAST_N_VIRGINIA';

update costusage.${environment}_usage.fact_dbrusage
set entity_type='COMPUTE SERVERLESS'
where sku_name ='ENTERPRISE_ALL_PURPOSE_SERVERLESS_COMPUTE_US_EAST_N_VIRGINIA';



UPDATE costusage.${environment}_usage.fact_dbrusage
SET usage_cost = usage_quantity*sku_listed_rate
, usage_cost_discounted = usage_quantity*sku_contract_rate
where sku_name ='ENTERPRISE_ALL_PURPOSE_SERVERLESS_COMPUTE_US_EAST_N_VIRGINIA';

-- COMMAND ----------

-- DBTITLE 1,Update the Missing Tag data

update costusage.${environment}_usage.fact_dbrusage set  CostGroup1 = 'DATABRICKS-US',CostGroup2 = 'PAS-US-DATABRICKS', Agency ='PMX'
where entity_id IN('5f557fc442276d4b','035ac6f21ccc1964','992760803193755','755918010195069','624011171531522','1066130183161618','653911635989676','319299244964279') and CostGroup2 is NULL; --7

update costusage.${environment}_usage.fact_dbrusage set CostGroup1 = 'DATABRICKS-US',CostGroup2 = 'DTI-US-DATABRICKS', Agency ='DTI'
where entity_id IN ('10a4194063759e3e','1017-132528-asb7xypf','4280ddc76cc9c8c5','0205-214648-ho38ymfl','0828-200031-x41luawz','420ff315adb2ac4b','211309913079754','0725-000039-24m8il3s','1205-215642-8pec9xx3','419b5d44cdd383fe','0904-173807-5d2ttobg','7018311965ab23a9','0904-183030-vd11d530','440345718949297','0807-192658-q147sub6','0830-175939-wlh7us8u','e46f166c2d7b1f8f','0725-004916-m0ibwzzo','402b96c44dbbc909','727020076788137','0727-195903-jwjevzeh','0725-004916-m0ibwzzo','60f6a728df553076','682a33ca7925dbea','0207-193402-9stz71qw','80b8b0590afa530d','80b8b0590afa530d','10a4194063759e3e') and CostGroup2 is NULL; --57

update costusage.${environment}_usage.fact_dbrusage set CostGroup1 = 'DATABRICKS-US',CostGroup2 = 'CMS-US-DATABRICKS', Agency ='DTI' where entity_id IN ('716544031961910','143586412148325') and CostGroup2 is NULL; --1

update costusage.${environment}_usage.fact_dbrusage set CostGroup1 = 'DATABRICKS-US',CostGroup2 = 'DTI-US-DATABRICKS', Agency ='DTI',Project ='AutLoadRunner-Manual' where entity_id ='577200325812575'; --1

update costusage.${environment}_usage.fact_dbrusage set CostGroup1 = 'DATABRICKS-US',CostGroup2 = 'DTI-US-DATABRICKS', Agency ='DTI',Project ='DCM_Aggregate_test_bn' where entity_id ='471665173815680'; --3

update costusage.${environment}_usage.fact_dbrusage set CostGroup1 = 'DATABRICKS-US',CostGroup2 = 'DTI-US-DATABRICKS', Agency ='DTI',Project ='DBM_Daily_New' where entity_id ='349655591459924'; --3


update costusage.${environment}_usage.fact_dbrusage set CostGroup1 = 'DATABRICKS-US',CostGroup2 = 'DTI-US-DATABRICKS', Agency ='DTI',Project ='VECTOR_SEARCH' where (entity_id is null or  entity_id like '%endpoint') and sku_name ='ENTERPRISE_SERVERLESS_REAL_TIME_INFERENCE_US_EAST_N_VIRGINIA' and CostGroup2 is NULL and workspace_id in ('1512882422162025'); 

update costusage.${environment}_usage.fact_dbrusage set CostGroup1 = 'DATABRICKS-US',CostGroup2 = 'DTI-US-DATABRICKS', Agency ='DTI',Project ='DLT Pipeline' where (entity_id is null or entity_id like 'pipelines%') and CostGroup2 is NULL and workspace_id in ('3339421385882115') and sku_name = 'ENTERPRISE_SERVERLESS_SQL_COMPUTE_US_EAST_N_VIRGINIA'; 


update costusage.${environment}_usage.fact_dbrusage set CostGroup1 = 'DATABRICKS-US',CostGroup2 = 'DTI-US-DATABRICKS', Agency ='DTI' where (workspace_id ='7509956532948377' or workspace_id ='1512882422162025')  and CostGroup2 is NULL; --DEV and TEST 14

update costusage.${environment}_usage.fact_dbrusage set CostGroup1 = 'DATABRICKS-US'
where CostGroup1 is NULL;

-- COMMAND ----------

--current month
select * from costusage.${environment}_usage.v_fact_dbrusage_current where costgroup2 is null;



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
  list_sku.sku_listed_rate,
  case when sku.sku_contract_rate is null then list_sku.sku_listed_rate else sku.sku_contract_rate end as sku_contract_rate,
  sku.entity_type,
  -- workspace.workspace,
  -- workspace.workspace_url,
  case 
  when usage_metadata.job_id is not null then concat('jobs/',usage_metadata.job_id)
  when usage_metadata.warehouse_id is not null then concat('sql/warehouses/',usage_metadata.warehouse_id)
  when usage_metadata.endpoint_id is not null and usage.billing_origin_product ='VECTOR_SEARCH' then concat('compute/vector-search/',usage_metadata.endpoint_name)
  when custom_tags.EndpointId is not null and usage.billing_origin_product ='MODEL_SERVING' then concat('ml/endpoints/',custom_tags.EndpointId)
  when usage_metadata.notebook_id is not null then concat('#notebook/',usage_metadata.notebook_id)
  when usage_metadata.instance_pool_id is not null then concat('#instance_pool/',usage_metadata.instance_pool_id)
  when usage_metadata.dlt_pipeline_id is not null then concat('pipelines/',usage_metadata.dlt_pipeline_id)
  else concat('compute/clusters/', usage_metadata.cluster_id)  end as entity_id

FROM
  system.billing.usage as usage
left join
  costusage.source_usage.dim_skupricing as sku on usage.sku_name = sku.sku_name and sku.enddate is null
  left join 
 (select account_id,price_start_time as startdate,price_end_time as enddate,sku_name,pricing.default as sku_listed_rate from system.billing.list_prices) as list_sku on usage.sku_name = list_sku.sku_name and list_sku.enddate is null
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
  When (ltrim(tags.CostGroup2) IS  null and (workspace_id = 5445138883374470 or workspace_id = 2838434924534453))  OR (upper(tags.CostGroup2) LIKE '%DPR%') THEN 'DPR-US-DATABRICKS'
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

-- MAGIC %md
-- MAGIC # v_fact_dbrusage_current

-- COMMAND ----------


CREATE OR REPLACE VIEW costusage.${environment}_usage.v_fact_dbrusage_current
as
WITH initalusage AS (
select 
  usage.*,
  list_sku.sku_listed_rate,
  case when sku.sku_contract_rate is null then list_sku.sku_listed_rate else sku.sku_contract_rate end as sku_contract_rate,
  sku.entity_type,
  -- workspace.workspace,
  -- workspace.workspace_url,
  case 
  when usage_metadata.job_id is not null then concat('jobs/',usage_metadata.job_id)
  when usage_metadata.warehouse_id is not null then concat('sql/warehouses/',usage_metadata.warehouse_id)
  when usage_metadata.endpoint_id is not null and usage.billing_origin_product ='VECTOR_SEARCH' then concat('compute/vector-search/',usage_metadata.endpoint_name)
  when custom_tags.EndpointId is not null and usage.billing_origin_product ='MODEL_SERVING' then concat('ml/endpoints/',custom_tags.EndpointId)
  when usage_metadata.notebook_id is not null then concat('#notebook/',usage_metadata.notebook_id)
  when usage_metadata.instance_pool_id is not null then concat('#instance_pool/',usage_metadata.instance_pool_id)
  when usage_metadata.dlt_pipeline_id is not null then concat('pipelines/',usage_metadata.dlt_pipeline_id)
  else concat('compute/clusters/', usage_metadata.cluster_id)  end as entity_id
FROM
  system.billing.usage as usage
left join
  costusage.source_usage.dim_skupricing as sku on usage.sku_name = sku.sku_name and sku.enddate is null
left join 
 (select account_id,price_start_time as startdate,price_end_time as enddate,sku_name,pricing.default as sku_listed_rate from system.billing.list_prices) as list_sku on usage.sku_name = list_sku.sku_name and list_sku.enddate is null
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
  When (ltrim(tags.CostGroup2) IS  null and (workspace_id = 5445138883374470 or workspace_id = 2838434924534453))  OR (upper(tags.CostGroup2) LIKE '%DPR%') THEN 'DPR-US-DATABRICKS'
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

select * from  costusage.source_usage.dim_contracts

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

-- DBTITLE 1,Missing SKU
select sku_name,sku_listed_rate,sku_contract_rate,min(usagedate) as startdate,max(usagedate) as enddate from costusage.${environment}_usage.fact_dbrusage 
group by sku_name,sku_listed_rate,sku_contract_rate--where entity_type is null

-- COMMAND ----------

select * from system.billing.list_prices where sku_name='ENTERPRISE_ALL_PURPOSE_SERVERLESS_COMPUTE_US_EAST_N_VIRGINIA'

-- COMMAND ----------

-- DBTITLE 1,Update Missing or Changing Sku Pricing
MERGE INTO costusage.${environment}_usage.fact_dbrusage as target
USING costusage.source_usage.dim_skupricing as source
ON source.sku_name = target.sku_name and target.usagedate >= source.startdate and (target.usagedate <= source.enddate or source.enddate is null) and (target.sku_contract_rate != source.sku_contract_rate
or target.sku_contract_rate is null)
WHEN MATCHED THEN
  UPDATE SET 
  target.sku_listed_rate = source.sku_listed_rate,
  target.sku_contract_rate = source.sku_contract_rate,
  target.entity_type = source.entity_type;
-- WHEN NOT MATCHED THEN
--   INSERT *
-- WHEN NOT MATCHED BY SOURCE THEN
--   DELETE

select * from  costusage.${environment}_usage.fact_dbrusage ;
update costusage.${environment}_usage.fact_dbrusage
set sku_listed_rate =  0.95,sku_contract_rate=0.95
where sku_listed_rate is null and sku_name ='ENTERPRISE_ALL_PURPOSE_SERVERLESS_COMPUTE_US_EAST_N_VIRGINIA';



UPDATE costusage.${environment}_usage.fact_dbrusage
SET usage_cost = usage_quantity*sku_listed_rate
, usage_cost_discounted = usage_quantity*sku_contract_rate
where usage_cost != usage_quantity*sku_listed_rate;

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
and dbr.yearmonth <=  to_date(cts_parent.dateto,"yyyy-MM-dd");
 

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
and dbr.yearmonth <=  to_date(cts_parent.dateto,"yyyy-MM-dd");

-- COMMAND ----------

select * from costusage.${environment}_usage.v_dbrusage_cumulative 
where year = year(current_date()) and month = month(current_date())

-- COMMAND ----------

-- DBTITLE 1,unexpected data auditing
--select distinct CostGroup from costusage.prod_usage.v_aggr_dbrusage_bycostgroup_current

select year,month,usagedate,workspace_id,Agency,CostGroup1,CostGroup2,Project,Client,sku_name,entity_id,usage_cost from costusage.${environment}_usage.v_fact_dbrusage_current --where CostGroup2 = "PMX"

-- COMMAND ----------

-- DBTITLE 1,dashboard query to use
select
  current_date() UpdatedBy,
  -- CostGroup,
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
group by year,month
order by year desc, month desc
