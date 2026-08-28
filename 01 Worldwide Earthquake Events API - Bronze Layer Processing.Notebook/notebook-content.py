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

import requests
import json

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#from datetime import date, timedelta

#start_date = date.today() - timedelta(7)
#end_date = date.today() - timedelta(1)

#display(start_date)
#display(end_date)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

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


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
