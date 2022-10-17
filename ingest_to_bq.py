import logging
import argparse
import csv

from datetime import datetime

import apache_beam as beam
from apache_beam.io.gcp.internal.clients import bigquery


class FilterColumns(beam.DoFn):

    def __init__(self, indices_list):
        self.indices = [int(x) for x in indices_list]

    def process(self, row):
        bq_rows = {}

        try:

            bq_rows['date'] = datetime.strptime(row[self.indices[0]], '%Y-%m-%d %H:%M:%S').date()
            bq_rows['country_region'] = str(row[self.indices[1]])
            bq_rows['province_state'] = str(row[self.indices[2]])
            bq_rows['confirmed'] = int(row[self.indices[3]]) if row[self.indices[3]] != '' else ''
            bq_rows['deaths'] = int(row[self.indices[4]]) if row[self.indices[4]] != '' else ''
            bq_rows['recovered'] = int(row[self.indices[5]]) if row[self.indices[5]] != '' else ''

            yield bq_rows
        except ValueError as error:
            logging.error(f"Issues in the record: {row} -> {str(error)}")
            # TO DO: implement errors bucket push


def parse_file(element):
    for line in csv.reader([element], quotechar='"', delimiter=',', quoting=csv.QUOTE_ALL, skipinitialspace=True):
        return line


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
    field.mode = 'nullable'
    table_schema.fields.append(field)

    field = bigquery.TableFieldSchema()
    field.name = 'province_state'
    field.type = 'string'
    field.mode = 'nullable'
    table_schema.fields.append(field)

    field = bigquery.TableFieldSchema()
    field.name = 'confirmed'
    field.type = 'int64'
    field.mode = 'nullable'
    table_schema.fields.append(field)

    field = bigquery.TableFieldSchema()
    field.name = 'deaths'
    field.type = 'int64'
    field.mode = 'nullable'
    table_schema.fields.append(field)

    field = bigquery.TableFieldSchema()
    field.name = 'recovered'
    field.type = 'int64'
    field.mode = 'nullable'
    table_schema.fields.append(field)

    return table_schema


def run(argv=None):

    parser = argparse.ArgumentParser()

    parser.add_argument('--input_file', required=True)
    parser.add_argument('--dest_table', required=True, help=(
        'Output BigQuery table for results specified as: DATASET.TABLE.'))
    parser.add_argument('--indices_list', required=True, help='list of indices to filter it out')

    known_args, pipeline_args = parser.parse_known_args(argv)

    with beam.Pipeline(argv=pipeline_args) as pipeline:

        file_contents = pipeline | 'Read from CSV' >> beam.io.ReadFromText(file_pattern=known_args.input_file, skip_header_lines=1)
        list_of_rows = file_contents | 'Split CSV into lists' >> beam.Map(parse_file)
        filtered_rows = list_of_rows | 'Filter columns from rows' >> beam.ParDo(
            FilterColumns(known_args.indices_list.split(',')))
        filtered_rows | 'Write into BigQuery' >> beam.io.WriteToBigQuery(
            known_args.dest_table,
            schema=get_table_schema(),
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
