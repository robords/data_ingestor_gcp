import pandas as pd
from datetime import date, datetime
from google.cloud import bigquery
from google.cloud import storage
import io


def get_most_recent_blob(bucket_name):
    """Lists all the blobs in the bucket.
    https://stackoverflow.com/questions/55509340/how-to-access-the-latest-uploaded-object-in-google-cloud-storage-bucket-using-py
    """
    # bucket_name = "your-bucket-name"
    storage_client = storage.Client()

    latest = sorted([(blob, blob.updated) for blob in storage_client.list_blobs(bucket_name)],
                    key=lambda tup: tup[1])[-1][0].download_as_string()
    return latest


def write_to_bigquery(a, b):

    item = get_most_recent_blob('msds-434-robords-city-housing-data')
    burlington_vt_housing_data = pd.read_csv(io.StringIO(item.decode('utf-8'))).iloc[:, 1:]
    today_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    burlington_vt_housing_data['Added'] = today_time

    client = bigquery.Client()

    # Prepare a reference to a new dataset for storing the query results.
    dataset_id = "burlington_vt_housing"
    dataset_id_full = f"{client.project}.{dataset_id}"
    dataset = bigquery.Dataset(dataset_id_full)

    try:
        # Create the new BigQuery dataset.
        dataset = client.create_dataset(dataset)
    except:
        pass

    try:
        table = client.create_table(f"{dataset_id_full}.housing_data")  # API request
    except:
        table = client.get_table(f"{dataset_id_full}.housing_data")

    job_config = bigquery.LoadJobConfig()

    job = client.load_table_from_dataframe(
        burlington_vt_housing_data,
        destination=table,
        job_config=job_config
    )  # Make an API request.

    job.result()  # Wait for the job to complete.

    table = client.get_table(table)  # Make an API request.

    return "Loaded csv to bigquery"


