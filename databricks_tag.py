# Databricks notebook source
# DBTITLE 1,ag
# MAGIC %pip install requests

# COMMAND ----------

# Serverless Gernal purpose compute

import requests
import json

# Databricks instance and token
DATABRICKS_INSTANCE = "https://pm-di-us.cloud.databricks.com"
# TOKEN = dbutils.secrets.get(scope="your_scope", key="your_key")  # Securely manage your token

# Get authentication headers from the Databricks notebook context
def get_auth_headers():
    token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    return headers

# # Headers for API requests
# headers = {
#     "Authorization": f"Bearer {TOKEN}",
#     "Content-Type": "application/json"
# }

# Tags to add
new_tags = {
    "Project": "map-migrated",
    "Environment": "migJVJEROWFU1"
}

def get_clusters():
    url = f"https://pm-di-us.cloud.databricks.com/api/2.0/clusters/list"
    headers = get_auth_headers()
    response = requests.get(url, headers=headers)
    return response.json().get("clusters", [])

def add_tags_to_cluster(cluster_id, cluster_name, existing_tags):
    updated_tags = {**existing_tags, **new_tags}
    edit_url = f"https://pm-di-us.cloud.databricks.com/api/2.0/clusters/edit"

    for key, value in new_tags.items():
        if key not in existing_tags:
            updated_tags[key] = value
    
    if updated_tags == existing_tags:
        print(f"No new tags to add for cluster {cluster_id}")
        return {"status": "No update needed"}
    else:
        print(f"new tags to add for cluster {cluster_id}")
         # Get current cluster configuration
        cluster_config = get_cluster_config(cluster_id)
        # Update the custom_tags field
        cluster_config["custom_tags"] = updated_tags
        payload = {
        "cluster_id": cluster_id,
        "cluster_name": cluster_name,  # Ensure cluster_name is retained
        "custom_tags": updated_tags
        }
        headers = get_auth_headers()
        response = requests.post(edit_url, headers=headers, json=payload)
        return response.json()
        

    


# Get list of existing clusters
clusters = get_clusters()

# Iterate through each cluster and add tags
for cluster in clusters:
    cluster_id = cluster["cluster_id"]
    cluster_name = cluster["cluster_name"]  # Ensure cluster_name is retained
    existing_tags = cluster.get("custom_tags", {})
    response = add_tags_to_cluster(cluster_id, cluster_name, existing_tags)
    print(f"Response for cluster {cluster_name}: {response}")
