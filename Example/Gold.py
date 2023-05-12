# Databricks notebook source
# DBTITLE 1,Load methods
# MAGIC %run ./utils

# COMMAND ----------

# DBTITLE 1,1. Load the USERS data from your silver table to a Spark dataframe
db_schema = 'jcrosas'
users_df = spark.read.table(f"{db_schema}.silver_users")

# COMMAND ----------

# DBTITLE 1,2. Create a gold table showing a summary of the users' age displaying the average mean age
bucketizer = Bucketizer(splits=list(range(0,100,10)), inputCol='age', outputCol='age_bucket')
bucketedData = bucketizer.transform(users_df)

users_age_data = bucketedData.groupBy('age_bucket') \
                        .agg(count('id').alias('num_users'), \
                            avg('age').alias('average_age'), \
                            min('age').alias('minimum_age'), \
                            max('age').alias('maximum_age')) 
                            
display(users_age_data)

# COMMAND ----------

# DBTITLE 1,3. Create a gold table showing a summary of the user's weight and height by blood group
# Load biometric data
biometric_df = spark.read.table(f"{db_schema}.silver_biometric")

height_weight_by_bloodType = biometric_df.groupBy('bloodGroup').agg(count('id').alias('num_users'), \
                            avg('height').alias('average_height'), \
                            min('height').alias('minimum_height'), \
                            max('height').alias('maximum_height'), \
                            avg('weight').alias('average_weight') , \
                            min('weight').alias('minimum_weight'), \
                            max('weight').alias('maximum_weight')) 

display(height_weight_by_bloodType)

# COMMAND ----------

# DBTITLE 1,4. Store the data
# Create tables and save them
table_names = ['users_age_data', 'height_weight_by_bloodType']

# Create dataframes 
dataframe_list = [users_age_data, height_weight_by_bloodType]

# Save dataframes in tables
for i, df in enumerate(dataframe_list):
    create_table(df, table_names[i], 'gold', 'jcrosas')

# COMMAND ----------

# DBTITLE 1,Make sure all tables exist
# MAGIC %sql
# MAGIC SHOW TABLES FROM jcrosas;

# COMMAND ----------

# DBTITLE 1,Check on tables (type whatever table you would like to check)
# MAGIC %sql
# MAGIC SELECT * FROM jcrosas.gold_height_weight_by_bloodtype;

# COMMAND ----------


