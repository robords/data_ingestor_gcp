# msds_434_cloud_data_ingestor

### Cloud Functions
https://cloud.google.com/sdk/gcloud/reference/functions/deploy

To deploy the function, required files are main.py and requirements.txt

The generic command is:
```
gcloud functions deploy YOUR_FUNCTION_NAME \
[--gen2] \
--region=YOUR_REGION \
--runtime=YOUR_RUNTIME \
--source=YOUR_SOURCE_LOCATION \
--entry-point=YOUR_CODE_ENTRYPOINT \
TRIGGER_FLAGS
```
In the case of the function to ingest data:
```
gcloud functions deploy data-ingestor \
  --region=us-east4 \
  --runtime=python39 \
  --source=./ingest_to_cloud_storage \
  --entry-point=data_ingestor \
  --trigger-http \
  --no-allow-unauthenticated
```

In the case of the function to move data from Cloud Storage to Big Query:
```
gcloud functions deploy data-migrator \
  --region=us-east4 \
  --runtime=python39 \
  --source=./cloud_storage_to_big_query_function \
  --entry-point=write_to_bigquery \
  --trigger-event=google.storage.object.finalize \
  --trigger-resource=msds-434-robords-city-housing-data \
  --no-allow-unauthenticated \
  --ingress-settings=internal-only
```

### Cloud Scheduler

__Get the currently scheduled jobs__
`gcloud scheduler jobs list --location=us-east4`

__Example Command__
```
gcloud scheduler jobs create http city_data_ingestor \
  --location us-east4 \
  --schedule "0 0 10 * *" \
  --time-zone "America/New_York" \
  --uri "https://us-east4-msds-434-robords-oct.cloudfunctions.net/data-ingestor" \
  --http-method GET \
  --oidc-service-account-email myserviceaccount@${PROJECT_ID}.iam.gserviceaccount.com
```