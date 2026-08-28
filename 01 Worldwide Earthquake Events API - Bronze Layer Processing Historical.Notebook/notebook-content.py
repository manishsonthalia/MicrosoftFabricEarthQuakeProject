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
# META     },
# META     "warehouse": {
# META       "known_warehouses": []
# META     }
# META   }
# META }

# CELL ********************

from datetime import date, timedelta, datetime
import requests
import json

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# start_date = date(2023, 1, 1)
# final_date = date.today()

start_date = datetime.strptime(start_date, "%Y-%m-%d").date()

table_name = "earthquake_lh2.dbo.earthquake_events_silver"

if not spark.catalog.tableExists(table_name):
    final_date = date.today()
else:
    final_date = spark.sql(f"""
        SELECT COALESCE(MIN(time), CURRENT_DATE()) AS min_date
        FROM {table_name}
    """).collect()[0]["min_date"]

final_date = final_date.date()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

while start_date < final_date:

    # Step the end date forward one day at a time until count hits 20000
    end_date = start_date + timedelta(days=1)
    count = 0

    while end_date <= final_date:
        count_url = (
            f"https://earthquake.usgs.gov/fdsnws/event/1/count?"
            f"starttime={start_date}&endtime={end_date}"
        )
        r = requests.get(count_url)
        c = int(r.text.strip())

        if c >= 20000:
            end_date = end_date - timedelta(days=1)   # step back to last good one
            break

        count = c
        end_date = end_date + timedelta(days=1)

    if end_date > final_date:
        end_date = final_date

    # display(start_date)
    # display(end_date)
    # display(count)

    """
    Fetch earthquake data from USGS for the specified date range
    and save the results as a JSON file in the Lakehouse.
    """

    # Construct the API URL
    eq_url = (
        f"https://earthquake.usgs.gov/fdsnws/event/1/query?"
        f"format=geojson&starttime={start_date}&endtime={end_date}"
    )

    #display(eq_url)

    # Make the GET request
    response = requests.get(eq_url)

    # Check if the request was successful
    if response.status_code == 200:
        # Get the JSON response and extract features
        data = response.json()["features"]
    #    display(data)

        # Specify the output file path
        file_path = f"/lakehouse/default/Files/{start_date}_earthquake_data.json"
    #    display(file_path)

        # Save the data as formatted JSON
        with open(file_path, "w") as file:
            json.dump(data, file, indent=4)

        print(f"Data successfully saved to {file_path}")
    else:
        print(f"Failed to fetch data. Status Code: {response.status_code}")

    # Restart from where this window ended
    start_date = end_date

# df = spark.sql("SELECT * FROM earthquake_lh2.dbo.earthquake_events_silver LIMIT 1000")
# display(df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
