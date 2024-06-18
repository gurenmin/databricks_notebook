-- Databricks notebook source
-- MAGIC %md
-- MAGIC # meta data

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
-- MAGIC ## retag by id

-- COMMAND ----------

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
-- MAGIC ## retag by name

-- COMMAND ----------

create or replace VIEW costusage.${environment}_usage.v_awscur_previous_retagbyIDname
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
-- MAGIC ## report view

-- COMMAND ----------

-- DBTITLE 1,rpt view: v_awscur_previous_reallocated
create 
--MATERIALIZED 
or replace
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

-- DBTITLE 1,rpt_view: v_fact_awscur_previous
-- DROP MATERIALIZED VIEW costusage.${environment}_usage.v_fact_awscur_previous;
create
or replace VIEW costusage.${environment}_usage.v_fact_awscur_previous as
SELECT
  year,
  month,
  usage_time,
  bill_billing_entity,
  owner_id,
  tag_costgroup1,
  tag_costgroup2,
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

-- DBTITLE 1,rpt view: v_fact_awscur_current

create or replace view costusage.${environment}_usage.v_fact_awscur_current
as
SELECT * FROM costusage.${environment}_usage.v_awscur_previous_reallocated
where (month = month(date_format(dateadd(MONTH, 0, current_date()), 'yyyy-MM-01')) and year = year(date_format(dateadd(MONTH, 0, current_date()), 'yyyy-MM-01')))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## options: select month

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

-- DBTITLE 1,retag by id
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
-- , ri_rate
-- , sp_rate
-- , ri_net_effective_cost
-- , sp_net_effective_cost
-- , ri_effective_cost
-- , sp_effective_cost
, product_sku
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

-- DBTITLE 1,retag by name
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
, product_sku
-- , ri_rate
-- , sp_rate
-- , ri_net_effective_cost
-- , sp_net_effective_cost
-- , ri_effective_cost
-- , sp_effective_cost
, original_tagcostgroup2 
, original_tagproject 
from costusage.${environment}_usage.v_awscur_selected_retagbyid
where (tag_name is not null and tag_name <> "")
WINDOW w as (partition by tag_name order by usage_time desc)
UNION ALL
select * from costusage.${environment}_usage.v_awscur_selected_retagbyid where (tag_name is null or tag_name ="")


-- COMMAND ----------

-- DBTITLE 1,rpt view v_awscur_selected_reallocated
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
, product_sku
-- , ri_rate
-- , sp_rate
-- , ri_net_effective_cost
-- , sp_net_effective_cost
-- , ri_effective_cost
-- , sp_effective_cost
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

-- DBTITLE 1,rpt view v_fact_awscur_selected

create or replace view costusage.${environment}_usage.v_fact_awscur_selected
as
SELECT year,
  month,
  usage_time,
  bill_billing_entity,
  owner_id,
  tag_costgroup1,
  tag_costgroup2,
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
, product_sku FROM costusage.${environment}_usage.v_awscur_selected_reallocated
where date_format(date(CONCAT(year, '-', LPAD(month, 2, '0'), '-01')), 'yyyy-MM-01') >= date_format(dateadd(MONTH, int(${from_selected}), current_date()), 'yyyy-MM-01') and date_format(date(CONCAT(year, '-', LPAD(month, 2, '0'), '-01')), 'yyyy-MM-01') < date_format(dateadd(MONTH, int(${to_selected}+1), current_date()), 'yyyy-MM-01')
--and service_name ='Amazon Registrar' --check registray


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## generate fact_awscur table

-- COMMAND ----------

-- DBTITLE 1,fact_awscur_previous
DROP TABLE IF EXISTS costusage.${environment}_usage.fact_awscur_previous;
create or replace table costusage.${environment}_usage.fact_awscur_previous
using delta
LOCATION 's3://pm-epsilon-athena/databricks/${environment}/${environment}_usage/fact_awscur_previous/'
AS
select * from costusage.${environment}_usage.v_fact_awscur_previous -- where (1=0);

-- COMMAND ----------

-- DBTITLE 1,retag with customtags
UPDATE costusage.${environment}_usage.fact_awscur_previous
SET tag_costgroup2 = case when upper(ltrim(rtrim(tag_scope))) ='EMEADATALAKE' then 'DATALAKE EMEA' else upper(ltrim(rtrim(COALESCE(tag_costgroup2,'')))) end
where tag_costgroup2 != upper(ltrim(rtrim(COALESCE(tag_costgroup2,'')))) or (upper(ltrim(rtrim(tag_scope))) ='EMEADATALAKE' and tag_costgroup2 <> 'DATALAKE EMEA');

MERGE INTO costusage.${environment}_usage.fact_awscur_previous AS aws
USING costusage.source_usage.dim_customtags AS dim
ON aws.owner_id = dim.name
AND dim.type = 'owner_id' 
AND dim.original = 'SA'
WHEN MATCHED THEN 
UPDATE SET aws.tag_costgroup2 = upper(trim(dim.new));


MERGE INTO costusage.${environment}_usage.fact_awscur_previous AS aws
USING costusage.source_usage.dim_customtags AS dim
ON aws.owner_id = dim.name
AND dim.type = 'owner_id' 
AND dim.original = 'CostGroup2'
AND aws.tag_costgroup2 = ''
WHEN MATCHED THEN 
UPDATE SET aws.tag_costgroup2 = upper(trim(dim.new));

MERGE INTO costusage.${environment}_usage.fact_awscur_previous AS aws
USING costusage.source_usage.dim_customtags AS dim
ON aws.tag_costgroup2 = upper(trim(dim.original))
AND dim.type = 'tags' 
AND dim.name = 'CostGroup2'
and aws.tag_costgroup2 <> upper(trim(dim.new))
WHEN MATCHED THEN 
UPDATE SET aws.tag_costgroup2 = upper(trim(dim.new));

MERGE INTO costusage.${environment}_usage.fact_awscur_previous AS aws
USING costusage.source_usage.dim_customtags AS dim
ON aws.owner_id = dim.original
AND dim.type = 'resource_id' 
AND aws.resource_id like dim.name
and aws.tag_costgroup2 <> upper(trim(dim.new))
WHEN MATCHED THEN 
UPDATE SET aws.tag_costgroup2 = upper(trim(dim.new));

UPDATE costusage.${environment}_usage.fact_awscur_previous
SET tag_costgroup2 = upper(trim(COALESCE(tag_costgroup2,'')))
where tag_costgroup2 <> upper(trim(COALESCE(tag_costgroup2,'')))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # insert new data

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

-- DBTITLE 1,aggrgate previous month date

delete from costusage.${environment}_usage.fact_awscur
where year*100+month in (select year*100+month from costusage.${environment}_usage.v_fact_awscur_previous);


insert into costusage.${environment}_usage.fact_awscur(year,month,line_)
select * from costusage.${environment}_usage.fact_awscur_previous
where year*100+month not in (select year*100+month from  costusage.${environment}_usage.fact_awscur);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### TAG REALLOCATED DONE
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # EC2/RN BENEFIT REALLOCATE

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md
-- MAGIC # query for USAA
-- MAGIC

-- COMMAND ----------

SELECT
    year,
    month,
    tag_costgroup2,
    CASE
        WHEN tag_name LIKE 'OPS-%' or tag_name LIKE 'CMS-%' THEN SUBSTRING(tag_name, 11, LEN(tag_name) - 10)
        WHEN aws_service != 'AmazonEC2' THEN SPLIT_PART(resource_id, '522531733883:', -1) 
        ELSE tag_name    
        END AS tag_name,
    tag_project,
    aws_service,
    service_family,
    instance_type,
    service_region,
    SUM(net_unblended_cost) AS net_unblended_cost,
    SUM(usage_amount) AS usage_amount,
    SUM(ondemand_cost) AS ondemand_cost,
    CASE
        WHEN tag_sp LIKE 'Y%' AND service_family = 'Compute Instance' THEN SPLIT_PART(tag_sp, ',', 5) - SPLIT_PART(tag_sp, ',', 4)
        ELSE 0
    END AS adjusted_rate,
    (SUM(ondemand_cost) + CASE
                            WHEN tag_sp LIKE 'Y%' AND service_family = 'Compute Instance' THEN SPLIT_PART(tag_sp, ',', 5) - SPLIT_PART(tag_sp, ',', 4)
                            ELSE 0
                        END * SUM(usage_amount)) * 0.82 AS direct_usage,
    tag_sp
FROM
    costusage.${environment}_usage.fact_awscur
WHERE
    tag_costgroup2 = 'SAPIENT USAA'
GROUP BY
    year,
    month,
    CASE
        WHEN tag_name LIKE 'OPS-%' or tag_name LIKE 'CMS-%' THEN SUBSTRING(tag_name, 11, LEN(tag_name) - 10)
        WHEN aws_service != 'AmazonEC2' THEN SPLIT_PART(resource_id, '522531733883:', -1) 
        ELSE tag_name    
        END,
    tag_costgroup2,
    tag_sp,
    tag_project,
    aws_service,
    service_family,
    instance_type,
    service_region;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # query report for Italy
-- MAGIC

-- COMMAND ----------


select 
year,
month,
date_format(dateadd(Month, 0, line_item_usage_start_date), 'yyyy-MM-01') as date,
-- bill_billing_entity,
'Agency Cross Charge' as tag_costgroup1,
tag_costgroup2,
tag_name,
resource_id as resource,
tag_italyambiente ,
tag_italycliente,
tag_italycostgroup1,
tag_italycostgroup2,
tag_product as tag_italyproduct,
aws_service,
service_family,
-- service_region,
-- service_os,
-- instance_type,
-- pricing_term,
-- pricing_unit,
-- ondemand_rate,
usage_amount,
ondemand_cost * 0.82 net_ondemand_cost
from costusage.${environment}_usage.fact_awscur 
where tag_costgroup2 ='VIVAKI ITALY' 
and ondemand_cost != 0 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #   query report for pas

-- COMMAND ----------

-- DBTITLE 1,v_fact_awscur_selected
-- years = [2003, 2021]
-- months = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]

-- for year in years:
--     for month in months:

       

-- INSERT INTO costusage.${environment}_usage.fact_aws_selected (year, month, tag_costgroup2, tag_name, tag_project, aws_service, service_family, service_region, net_unblended_cost, usage_amount, ondemand_cost, adjusted_rate, direct_usage, tag_sp)
SELECT
    year,
    month,
    tag_costgroup2,
    CASE
        WHEN tag_name LIKE 'OPS-%' THEN SUBSTRING(tag_name, 11, LEN(tag_name) - 10)
        WHEN aws_service = 'AmazonS3' THEN resource_id
        ELSE tag_name
    END AS tag_name,
    tag_project,
    aws_service,
    service_family,
    service_region,
    SUM(net_unblended_cost) AS net_unblended_cost,
    SUM(usage_amount) AS usage_amount,
    SUM(ondemand_cost) AS ondemand_cost,
    CASE
        WHEN tag_sp LIKE 'Y%' AND service_family = 'Compute Instance' THEN SPLIT_PART(tag_sp, ',', 5) - SPLIT_PART(tag_sp, ',', 4)
        ELSE 0
    END AS adjusted_rate,
    (SUM(ondemand_cost) + CASE
                            WHEN tag_sp LIKE 'Y%' AND service_family = 'Compute Instance' THEN SPLIT_PART(tag_sp, ',', 5) - SPLIT_PART(tag_sp, ',', 4)
                            ELSE 0
                        END * SUM(usage_amount)) * 0.82 AS direct_usage,
    tag_sp
FROM
    costusage.${environment}_usage.v_fact_awscur_selected
WHERE
    original_tagcostgroup2 IN ('PMX', 'PRECISION', 'PRECISION ANALYTICS SUIT', 'SITECORE') OR tag_costgroup2 = 'PRECISION'
    -- AND year = {{year}} AND month = {{month}}
GROUP BY
    year,
    month,
    tag_costgroup2,
    CASE
        WHEN tag_name LIKE 'OPS-%' THEN SUBSTRING(tag_name, 11, LEN(tag_name) - 10)
        WHEN aws_service = 'AmazonS3' THEN resource_id
        ELSE tag_name
    END,
    tag_sp,
    tag_project,
    aws_service,
    service_family,
    service_region;

-- COMMAND ----------

select * from costusage.${environment}_usage.v_fact_awscur_previous LIMIT 10


-- COMMAND ----------

-- DBTITLE 0,validate
-- REFRESH MATERIALIZED VIEW costusage.${environment}_usage.v_fact_awscur_previous;


select
  year,
  month,
  original_tagcostgroup2,
  tag_costgroup2,case
    when tag_name like 'OPS-%' THEN substring(tag_name, 11, len(tag_name) -10)
    when aws_service = 'AmazonS3' then resource_id
    else tag_name
  end as tag_name,
  tag_project,
  aws_service,
  service_family,
  service_region,
  sum(net_unblended_cost) net_unblended_cost,
  sum(usage_amount) usage_amount,
  sum(ondemand_cost) ondemand_cost,
  case
    when tag_sp like 'Y%'
    and service_family = 'Compute Instance' then SPLIT_PART(tag_sp, ',', 5) - SPLIT_PART(tag_sp, ',', 4)
    else 0
  end as adjusted_rate,
  (
    sum(ondemand_cost) +case
      when tag_sp like 'Y%'
      and service_family = 'Compute Instance' then SPLIT_PART(tag_sp, ',', 5) - SPLIT_PART(tag_sp, ',', 4)
      else 0
    end * sum(usage_amount)
  ) * 0.82 as direct_usage,
  tag_sp
from
  costusage.${environment}_usage.v_fact_awscur_previous
where
  original_tagcostgroup2 = 'PMX'
  OR original_tagcostgroup2 = 'SITECORE'
group by
  year,
  month,
  tag_costgroup2,case
    when tag_name like 'OPS-%' THEN substring(tag_name, 11, len(tag_name) -10)
    when aws_service = 'AmazonS3' then resource_id
    else tag_name
  end,
  tag_sp,
  tag_project,
  aws_service,
  service_Family,
  service_region,
  original_tagcostgroup2

-- COMMAND ----------

select
  'v_awscur_previous',
  count(1)
from
  costusage.${environment} _usage.v_awscur_previous
where
  tag_costgroup2 = 'PRECISION'
union all
select
  'v_awscur_previous_retagbyid',
  count(1)
from
  costusage.${environment} _usage.v_awscur_previous_retagbyid
where
  original_tagcostgroup2 = 'PRECISION'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Audit

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


-- COMMAND ----------

 select * from costusage.${environment}_usage.v_aggr_awscur_previous 
 where tag_costgroup2 = 'PRECISION'

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

CREATE OR REPLACE TABLE costusage.{{environment}}_usage.fact_awsusage
USING DELTA
LOCATION 's3://pm-epsilon-athena/databricks/{{environment}}/{{environment}}_usage/fact_awsusage/'
PARTITIONED BY (year , month)
select * from costusage.source_costusage.v_current_month_aws_cur

insert table costusage.{{environment}}_usage.fact_awsusage
select * from costusage.source_costusage.v_previous_month_aws_cur
