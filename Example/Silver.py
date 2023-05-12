# Databricks notebook source
# DBTITLE 1,Load methods
# MAGIC %run ./utils

# COMMAND ----------

# DBTITLE 1,1. Load the USERS data from your bronze table to a Spark dataframe
db_schema = 'jcrosas'
users_df = spark.read.table(f"{db_schema}.bronze_users")
display(users_df)

# COMMAND ----------

# DBTITLE 1,2. Identify the nested data: hair, address, bank, and company and explore Spark methods on dataframes to flatten these fields. 
for col in users_df.columns:
    if users_df.schema[col].dataType.typeName() == 'struct':
        print(col)

# COMMAND ----------

# DBTITLE 1,3 and 4. Update the dataframe to flatten all the nested records. Repeat the flattening process for all original nested data
users_df = flatten_sdf(users_df)
display(users_df)

# COMMAND ----------

# DBTITLE 1,5. Store the data of flattened dataframe 'users' in silver layer
create_table(users_df, 'users', 'silver', 'jcrosas')

# COMMAND ----------

# DBTITLE 1,Create 'personal', 'biometric', 'financial', and 'contact' tables
# Names of columns for each table
table_names = ['personal', 'biometric', 'financial', 'contact']
personal_fields, biometric_fields, financial_fields, contact_fields = \
    ['id', 'firstName', 'lastName', 'maidenName', 'username','birthDate', 'ssn', 'university', 'userAgent', 'image'], \
    ['id','age', 'bloodGroup', 'eyeColor', 'gender', 'hair_color', 'hair_type', 'height', 'weight'], \
    ['id','bank_cardNumber', 'bank_cardType','bank_cardExpire', 'bank_currency', 'bank_iban'], \
    ['id','email', 'phone', 'password', 'address_address', 'address_city', 'address_coordinates_lat', 'address_coordinates_lng', 'address_postalCode', 'address_state', 'company_address_address', \
     'company_address_city', 'company_address_coordinates_lat', 'company_address_coordinates_lng', 'company_address_postalCode', 'company_address_state', 'company_department', 'company_name', \
     'company_title', 'domain', 'ein', 'macAddress', 'ip']

# Create dataframes 
dataframe_list = [users_df.select(*item) for item in [personal_fields, biometric_fields, financial_fields, contact_fields]]

# Save dataframes in tables
for i, df in enumerate(dataframe_list):
    create_table(df, table_names[i], 'silver', 'jcrosas')

# COMMAND ----------

# DBTITLE 1,Make sure all tables exist
# MAGIC %sql
# MAGIC SHOW TABLES FROM jcrosas;

# COMMAND ----------

# DBTITLE 1,Check on tables (type whatever table you would like to check)
# MAGIC %sql
# MAGIC SELECT * FROM jcrosas.silver_biometric;

# COMMAND ----------


