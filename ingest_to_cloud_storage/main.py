#!/usr/bin/env python
# coding: utf-8

# ## Get Housing data from Burlington, VT's Open Data Repo
#
# https://maps.burlingtonvt.gov/portal/apps/sites/#/open-data/datasets/604e666d05e9496a82c524c9e027a4cb/data?orderBy=objectid&orderByAsc=false
#
# Example:
# ```
# URL = "https://maps.burlingtonvt.gov/arcgis/rest/services/Hosted/Assessor_Property_Building_Information/FeatureServer/0/query?where=1%3D1&outFields=*&outSR=4326&f=json"
# burlington_property = requests.get(URL)
#
# burlington_property.json()['exceededTransferLimit'] # if this is true, there are more pages
#
# burlington_property.json().keys()
# # Returns: dict_keys(['exceededTransferLimit', 'features', 'fields', 'objectIdFieldName', 'hasZ', 'hasM'])
#
# # get the last value from the data pulled:
# pd.json_normalize(burlington_property.json()['features']).iloc[-1]['attributes.objectid']
# ```
import requests
import pandas as pd
from datetime import date, datetime
from google.cloud import storage

def data_ingestor(request):

    start_value = 1
    ending_value = 2001
    property_df_list = []
    new_results = True

    while new_results:
        URL = f"https://maps.burlingtonvt.gov/arcgis/rest/services/Hosted/Assessor_Property_Building_Information/FeatureServer/0/query?where=objectid%20%3E%3D%20{start_value}%20AND%20objectid%20%3C%3D%20{ending_value}&outFields=*&outSR=4326&f=json"
        burlington_property_raw = requests.get(URL)
        property_df_list.append(pd.json_normalize(burlington_property_raw.json()['features']))
        new_results = burlington_property_raw.json()['exceededTransferLimit']
        start_value = pd.json_normalize(burlington_property_raw.json()['features']).iloc[-1]['attributes.objectid'] +1
        ending_value = start_value + 2001

    burlington_vt_housing_data = pd.concat(property_df_list)

    ## Columns in Bigquery can't have .
    burlington_vt_housing_data.columns = burlington_vt_housing_data.columns.str.replace(".", "_")


    # ## Write the Data to Cloud Storage
    def upload_blob(bucket_name, source_df, destination_blob_name):
        """Uploads a file to the bucket."""
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)

        #blob.upload_from_string(source_df.to_csv(), 'text/csv')
        blob.upload_from_string(source_df.to_csv(), 'text/csv')

        print('File uploaded to {}.'.format(
          destination_blob_name))


    today_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    upload_blob('msds-434-robords-city-housing-data', burlington_vt_housing_data,
              f'burlington_vt_housing_data_{today_time}.csv')

    return 'Successly wrote file to Cloud Storage'
