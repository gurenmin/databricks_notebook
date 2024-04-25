-- Databricks notebook source
-- MAGIC %md
-- MAGIC add widget

-- COMMAND ----------

CREATE WIDGET DROPDOWN environment 
DEFAULT 'dev' CHOICES SELECT * FROM (VALUES ("dev"), ("test"), ("prod"));


CREATE WIDGET DROPDOWN costgroup DEFAULT 'DTI-US-DATABRICKS' CHOICES SELECT distinct(ifnull(CostGroup,'DTI-US-DATABRICKS')) AS costgroup from costusage.prod_usage.v_dbrusage_cumulative_subcostgroup;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC put null as unspecifed in sub_costgroup?

-- COMMAND ----------

-- MAGIC %md
-- MAGIC distinct costgroup for dashboard widget

-- COMMAND ----------

select * from costusage.source_usage.dim_contracts

-- COMMAND ----------

-- MAGIC %md
-- MAGIC get contract group monthly usage

-- COMMAND ----------

-- DBTITLE 1,current usage
-- select
--   current_date() UpdatedBy,
--   CostGroup,
--   yearmonth,
--   sum(usage_cost) month_to_date_cost,
--   sum(month_budget) month_budget,
--   sum(annual_budget) annual_budget,
--   sum(commited) commited_cost,
--   case when sum(hard_limit) = 0 then sum(commited)*0.95
--   else sum(hard_limit) end as hard_limit,
--   sum(cumulative_usage_cost) cumulative_usage_cost
-- from
-- costusage.${environment}_usage.v_dbrusage_cumulative
-- where CostGroup = "${costgroup}"
-- group by CostGroup,yearmonth
-- order by yearmonth desc

select
  current_date() UpdatedBy,
  CostGroup,
  --ifnull(sub_costgroup, '-1') as sub_costgroup,
  yearmonth,
  sum(usage_cost) month_to_date_cost,
  avg(month_budget) month_budget,
  avg(annual_budget) annual_budget,
  avg(commited) commited_cost,
  case when avg(hard_limit) = 0 then sum(commited)*0.95
  else avg(hard_limit) end as hard_limit,
  avg(cumulative_usage_cost) cumulative_usage_cost
from
costusage.${environment}_usage.v_dbrusage_cumulative_subcostgroup
where CostGroup = "${costgroup}"
group by CostGroup,yearmonth
order by yearmonth desc


-- COMMAND ----------

select
  current_date() UpdatedBy,
  CostGroup,
  ifnull(sub_costgroup, '-1') as sub_costgroup,
  yearmonth,
  sum(usage_cost) month_to_date_cost,
  sum(month_budget) month_budget,
  sum(annual_budget) annual_budget,
  sum(commited) commited_cost,
  case when sum(hard_limit) = 0 then sum(commited)*0.95
  else sum(hard_limit) end as hard_limit,
  sum(cumulative_usage_cost) cumulative_usage_cost
from
costusage.${environment}_usage.v_dbrusage_cumulative_subcostgroup
where CostGroup = "${costgroup}"
group by CostGroup,sub_costgroup,yearmonth
order by yearmonth desc


-- COMMAND ----------

SELECT *
from costusage.${environment}_usage.v_fact_dbrusage
where CostGroup2 = "${costgroup}"
--line_item_resource_idand usagedate ='2023-10-01'
