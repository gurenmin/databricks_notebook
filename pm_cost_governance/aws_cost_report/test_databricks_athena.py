# Databricks notebook source
import boto3
region_name = 'us-east-1'
client = boto3.client('athena', region_name=region_name)


# COMMAND ----------

queryStart = client.start_query_execution(
    QueryString = 'select * from previous_month_costreportbycostgroup2 where tag_costgroup2 =""',
    QueryExecutionContext = {
        'Database': 'athenacurcfn_pm_epsilon_cur_athena'
    }, 
    ResultConfiguration = { 'OutputLocation': 's3:///pm-epsilon-athena/queryoutput'}
)
