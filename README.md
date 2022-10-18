# hopkins-data-ingestion
Demonstration of Hopkins covid data ingestion from Github to BigQuery
## Logical Flow
![Logical Flow](https://github.com/mihaque/hopkins-data-ingestion/blob/master/screenshots/hopkins_ingestion_logical_flow.png)


#### How to run the process


## How to run the dataflow
### For direct runner: 
python.exe ingest_to_bq_dataflow.py --input_file=csv_file_pattern --dest_table=hopkins_data.hopkins_staging --indices_list="4, 3, 2, 7, 8, 9" --temp_location=gs://temporary_location --runner=direct --project=gcloud-project --region=gcloud-dataflow-region-to-run

### For dataflow runner: 
python.exe ingest_to_bq_dataflow.py --input_file=csv_file_pattern --dest_table=hopkins_data.hopkins_staging --indices_list="4, 3, 2, 7, 8, 9" --temp_location=gs://temporary_location --runner=dataflow --project=gcloud-project --region=gcloud-dataflow-region-to-run
