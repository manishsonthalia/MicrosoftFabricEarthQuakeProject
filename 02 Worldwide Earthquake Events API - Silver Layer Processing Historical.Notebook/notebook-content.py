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

import os, glob, re

d = '/lakehouse/default/Files'

for p in sorted(glob.glob(os.path.join(d, '*_earthquake_data.json'))):
    name = os.path.basename(p)
    if ':' not in name:
        continue

    stamp = name.split('_earthquake_data.json')[0]      # 2026-08-23T03:18:55.3493894Z
    day    = stamp[:10]                                 # 2026-08-23
    hhmmss = re.sub(r'\D', '', stamp[11:19]) or '000000'  # 031855

    new = f"{day}_{hhmmss}_earthquake_data.json"
    n = 1
    while os.path.exists(os.path.join(d, new)):         # never clobber
        new = f"{day}_{hhmmss}_{n}_earthquake_data.json"
        n += 1

    os.rename(p, os.path.join(d, new))
    print(f"renamed: {name} -> {new}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# df now is a Spark Dataframe containing JSON data
df = spark.read.option("multiline","true").json("Files/*_earthquake_data.json")

df = df.dropDuplicates(['id'])

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

# Convert 'time' and 'updated' columns from milliseconds to timestamp format for clearer datetime representation.
df = df.\
    withColumn('time', col('time')/1000).\
    withColumn('updated', col('updated')/1000).\
    withColumn('time', col('time').cast(TimestampType())).\
    withColumn('updated', col('updated').cast(TimestampType()))


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
