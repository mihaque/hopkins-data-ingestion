import logging
import argparse
from datetime import datetime

import apache_beam as beam
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.dataframe.io import read_csv
from apache_beam.dataframe.convert import to_pcollection

class FilterColumns(beam.DoFn):

    def __init__(self, indices_list):
        self.indices = indices_list

    def process(self, row):
        bq_rows = {}
        try:
            bq_rows = {
                'date': datetime.strptime(row[int(self.indices[0])], '%Y-%m-%d %H:%M:%S').date(),
                'country_region': row[int(self.indices[1])],
                'province_state': row[int(self.indices[2])],
                'confirmed': row[int(self.indices[3])],
                'deaths': row[int(self.indices[4])],
                'recovered': row[int(self.indices[5])]
            }
            yield bq_rows
        except ValueError:
            logging.error(f"Issues in the record: {row}")
            # TO DO: implement errors bucket push


def get_table_schema():

    table_schema = bigquery.TableSchema()

    field = bigquery.TableFieldSchema()
    field.name = 'date'
    field.type = 'date'
    field.mode = 'required'
    table_schema.fields.append(field)

    field = bigquery.TableFieldSchema()
    field.name = 'country_region'
    field.type = 'string'
    field.mode = 'required'
    table_schema.fields.append(field)

    field = bigquery.TableFieldSchema()
    field.name = 'province_state'
    field.type = 'string'
    field.mode = 'nullable'
    table_schema.fields.append(field)

    field = bigquery.TableFieldSchema()
    field.name = 'confirmed'
    field.type = 'int64'
    field.mode = 'required'
    table_schema.fields.append(field)

    field = bigquery.TableFieldSchema()
    field.name = 'deaths'
    field.type = 'int64'
    field.mode = 'required'
    table_schema.fields.append(field)

    field = bigquery.TableFieldSchema()
    field.name = 'recovered'
    field.type = 'int64'
    field.mode = 'required'
    table_schema.fields.append(field)

    return table_schema


def run(argv=None):
    """Run the workflow."""
    parser = argparse.ArgumentParser()

    parser.add_argument('--input_file', required=True)
    parser.add_argument('--dest_table', required=True, help=(
        'Output BigQuery table for results specified as: PROJECT:DATASET.TABLE or DATASET.TABLE.'))
    parser.add_argument('--indices_list', required=True, help='list of indices to filter it out')

    known_args, pipeline_args = parser.parse_known_args(argv)

    table_spec = bigquery.TableReference(
        projectId='analytics-plp-dev',
        datasetId='test_mohaque',
        tableId='test_data'
    )

    with beam.Pipeline(argv=pipeline_args) as pipeline:

        file_contents = pipeline | 'Read from CSV' >> beam.io.ReadFromText(file_pattern=known_args.input_file, skip_header_lines=1)
        list_of_rows = file_contents | 'Split CSV into lists' >> beam.Map(lambda text_row: text_row.split(","))
        filtered_rows = list_of_rows | 'Filter columns from rows' >> beam.ParDo(
            FilterColumns(known_args.indices_list.split(',')))
        filtered_rows | 'Write into BigQuery' >> beam.io.WriteToBigQuery(
            table_spec,
            schema=get_table_schema(),
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
