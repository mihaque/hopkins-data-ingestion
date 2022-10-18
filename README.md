# hopkins-data-ingestion
Demonstration of Hopkins covid data ingestion from Github to BigQuery
## Logical Flow
![Logical Flow](https://github.com/mihaque/hopkins-data-ingestion/blob/master/screenshots/hopkins_ingestion_logical_flow.png)

Note: The orchestration layer development is pending. So for now we need to do manual run of the dataflow

#### How to run the process

* Install the dependecies using requirements.txt
* Download the data from Github
* Create dataset hopkins_data inside BQ
* Execute the dataflow using the python command and desired options
* Create a final table by copying staging using BQ table copy option. Keep the name as hopkins_final
* Execute merge query sql/merge_with_final.sql (we can plug this with Airflow once the orchestration layer complete)
* Execute view sql/country_wise_reports.sql inside BQ
* You can see the country wise reports 

## How to run the dataflow
### For direct runner: 
```
python ingest_to_bq_dataflow.py --input_file=csv_file_pattern --dest_table=hopkins_data.hopkins_staging --indices_list="4, 3, 2, 7, 8, 9" --temp_location=gs://temporary_location --runner=direct --project=gcloud-project --region=gcloud-dataflow-region-to-run
```

### For dataflow runner: 
```
python ingest_to_bq_dataflow.py --input_file=csv_file_pattern --dest_table=hopkins_data.hopkins_staging --indices_list="4, 3, 2, 7, 8, 9" --temp_location=gs://temporary_location --runner=dataflow --project=gcloud-project --region=gcloud-dataflow-region-to-run
```
