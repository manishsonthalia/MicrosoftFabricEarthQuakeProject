# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "9fd4b45c-0c21-4199-bd37-95a7ad633d66",
# META       "default_lakehouse_name": "earthquake_lh2",
# META       "default_lakehouse_workspace_id": "7d2a8cff-024f-4887-a8a2-045d4d1f9aca",
# META       "known_lakehouses": [
# META         {
# META           "id": "9fd4b45c-0c21-4199-bd37-95a7ad633d66"
# META         }
# META       ]
# META     },
# META     "environment": {
# META       "environmentId": "162c5fe9-d517-b8b6-4681-1bcf23ab8900",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql.functions import col
from pyspark.sql.types import TimestampType

# from datetime import date, timedelta

# start_date = date.today() - timedelta(1)
# print(start_date)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# df now is a Spark Dataframe containing JSON data
df = spark.read.option("multiline","true").json(f"Files/{start_date}_earthquake_data.json")

df = \
df.\
    select(
        'id',
        col('geometry.coordinates').getItem(0).alias('longitude'),
        col('geometry.coordinates').getItem(1).alias('latitude'),
        col('geometry.coordinates').getItem(2).alias('elevation'),
        col('properties.title').alias('title'),
        col('properties.place').alias('place_description'),
        col('properties.sig').alias('sig'),
        col('properties.mag').alias('mag'),
        col('properties.magType').alias('magType'),
        col('properties.time').alias('time'),
        col('properties.updated').alias('updated')
    )

# # Convert 'time' and 'updated' columns from milliseconds to timestamp format for clearer datetime representation.
df = df.\
    withColumn('time', col('time')/1000).\
    withColumn('updated', col('updated')/1000).\
    withColumn('time', col('time').cast(TimestampType())).\
    withColumn('updated', col('updated').cast(TimestampType()))

# display(df)
# df.write.mode('append').saveAsTable('earthquake_events_silver')



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from delta.tables import DeltaTable

table_name = "earthquake_events_silver"

if spark.catalog.tableExists(table_name):
    target = DeltaTable.forName(spark, table_name)

    (
        target.alias("target")
        .merge(
            df.alias("source"),
            "target.id = source.id"
        )
        .whenNotMatchedInsertAll()
        .execute()
    )
else:
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(table_name)
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
