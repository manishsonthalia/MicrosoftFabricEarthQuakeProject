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

# MARKDOWN ********************


# CELL ********************

from pyspark.sql.functions import when, col, pandas_udf   # CHANGED [1]: udf -> pandas_udf
from pyspark.sql.types import StringType
from delta.tables import DeltaTable
from datetime import date, timedelta
import pandas as pd                                       # ADDED [1]

# ensure reverse_geocoder is installed on your Fabric environment
import reverse_geocoder as rg


# -----------------------------------------------------------------------------
# Source
# -----------------------------------------------------------------------------
df = (
    spark.read.table("earthquake_events_silver")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -----------------------------------------------------------------------------
# Reverse geocoding
# -----------------------------------------------------------------------------
@pandas_udf(StringType())
def get_country_code(lat: pd.Series, lon: pd.Series) -> pd.Series:
    """
    Batch reverse-geocode latitude/longitude pairs to ISO country codes.

    Parameters:
    lat (pd.Series): Latitudes for this Arrow batch.
    lon (pd.Series): Longitudes for this Arrow batch.

    Returns:
    pd.Series: ISO 3166-1 alpha-2 country code per input row.
    """
    coords = list(zip(lat.astype(float), lon.astype(float)))
    if not coords:
        return pd.Series([], dtype="object")

    hits = rg.search(coords, mode=2, verbose=False)
    return pd.Series([h.get("cc") for h in hits], index=lat.index)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -----------------------------------------------------------------------------
# Transformations
# -----------------------------------------------------------------------------
df_with_location_sig_class = (
    df
    .withColumn("country_code", get_country_code(col("latitude"), col("longitude")))
    .withColumn(
        "sig_class",
        when(col("sig") < 100, "Low")
        .when(col("sig") < 500, "Moderate")
        .otherwise("High")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -----------------------------------------------------------------------------
# Merge into gold
# -----------------------------------------------------------------------------
table_name = "earthquake_events_gold"

src = df_with_location_sig_class.cache()

if spark.catalog.tableExists(table_name):
    (
        DeltaTable.forName(spark, table_name).alias("target")
        .merge(
            src.alias("source"),
            "target.id = source.id"
        )
        .whenNotMatchedInsertAll()
        .execute()
    )
else:
    (
        src.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(table_name)
    )

src.unpersist()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
