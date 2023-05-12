# Databricks notebook source
import json
import requests
import pyspark.sql
import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
from pyspark.ml.feature import Bucketizer
from pyspark.sql.functions import avg,max,min,count

# COMMAND ----------

def create_sdf_from_api_call(item: str, record_number: int=None):
    """
    Creates a Spark Dataframe from data retrieved from https://dummyjson.com.
        item: the item data that will be retrieved
        record_number: the number of records that will be retrieved. Default is all records
    """

    # Get data
    res = requests.get('https://dummyjson.com/' + item)
    limit = res.json()['limit']

    try:
        # Set number of records to be read
        if record_number is None:
            record_number = limit

        # Creates a list with the data of each record as JSON file
        jsonDataList = [json.dumps(res.json()[item][i]) for i in range(record_number)]

        # Retuns data as a Spark Dataframe
        return spark.read.json(sc.parallelize(jsonDataList))
    except IndexError:
        print('record_number cannot be > than ' + str(limit))

# COMMAND ----------

def flatten_sdf(df):
    """
    Method to flatten a Spark Dataframe. It employs the method "get_column_names"
        df: dataframe to be flattened
    """

    df_schema = df.schema    
    def get_column_names(schema, outer_field=None):
        """
        Recursive method to extract the names of all columns of a dataframe, included nested columns
            schema: schema of a dataframe (type is pyspark.sql.types.StructType)
            outer_field: outermost name of a nested column
        """
        fields = []
        for field in schema.fields:
            name = outer_field + '.' + field.name if outer_field else field.name
            dtype = field.dataType
            if isinstance(dtype, StructType):
                fields += get_column_names(dtype, outer_field=name)
            else:
                fields.append(name)
        return fields

    cols = get_column_names(df_schema)
    cols_rename = [col.replace('.', '_') for col in cols]
    return df.select(*cols).toDF(*cols_rename)

# COMMAND ----------

def create_table(sdf: DataFrame, table_name: str, layer: str, db_schema: str, loc: str=None,  flatten: bool=False):
    """
        Saves a Spark dataframe into a delta table.
            sdf: Spark dataframe to be saved
            table_name: the name of the table that will be created
            layer: the layer where the table will be created
            db_schema: the schema where the table will be created
            loc: location path for the schema. Default is /user/hive/warehouse/
    """

    if flatten is True:
        sdf = flatten_sdf(sdf)

    # Create schema if not exists
    if loc is None:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {db_schema}")
    else:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {db_schema} LOCATION {loc}")

    # DROP table if exists
    spark.sql(f"DROP TABLE IF EXISTS {db_schema}.{layer}_{table_name}")

    # Load data into table
    sdf.write.format("delta").mode("overwrite").saveAsTable(f"{db_schema}.{layer}_{table_name}")

# COMMAND ----------


