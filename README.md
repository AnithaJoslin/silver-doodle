# silver-doodle
When the csv file drops into the GCP bucket, the data is transferred to BigQuery table using Cloud function and Dataflow
The whole project works in 2 phases:
Phase 1:
Triggering the cloud function whenever the .csv file is placed in bucket. This triggers our dataflow pipeline
Phase 2:
The dataflow pipeline that picks up dat from the csv file and drop it into the given BigQuery table

Packages needed:
Cloud function:
functions-framework==3.*
google-api-python-client
Dataflow:
functions-framework==3.8.2
cloudevents==1.11.0
google-api-python-client==2.118.0
apache-beam[gcp]==2.64.0
