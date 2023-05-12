# Databricks notebook source
# DBTITLE 1,Load methods
# MAGIC %run ./utils

# COMMAND ----------

# DBTITLE 1,Make the call to the API and load the resource data to a Spark dataframe
# Create dataframes with only 5 entries, to make sure method "create_sdf" works
carts_df = create_sdf_from_api_call('carts')
users_df = create_sdf_from_api_call('users')

display(carts_df)
display(users_df)

# COMMAND ----------

# DBTITLE 1,Store the data
# Choose schema and layer
schema_name = 'jcrosas'
layer = 'bronze'

# Create tables
create_table(carts_df, 'carts', layer, schema_name)
create_table(users_df, 'users', layer, schema_name)

# COMMAND ----------

# DBTITLE 1,Make sure all tables exist
# MAGIC %sql
# MAGIC SHOW TABLES FROM jcrosas;

# COMMAND ----------

# DBTITLE 1,Check on tables (type whatever table you would like to check)
# MAGIC %sql
# MAGIC SELECT * FROM jcrosas.bronze_carts

# COMMAND ----------


