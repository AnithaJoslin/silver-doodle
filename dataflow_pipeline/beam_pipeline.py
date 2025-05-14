import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.runners import DataflowRunner
import argparse
import traceback

def run():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_path', type=str)
    parser.add_argument('--output_table', type=str)
    args, beam_args = parser.parse_known_args()
    schema = 'user_id:STRING, product_id:STRING, category:STRING, amount:FLOAT, timestamp:TIMESTAMP'
    options = PipelineOptions(beam_args, runner='DataflowRunner', streaming=True)

    p = beam.Pipeline(options=options)
    (
        p
        | 'Read CSV' >> beam.io.ReadFromText(args.input_path, skip_header_lines=1)
        | 'Split' >> beam.Map(lambda line: line.split(','))
        | 'To Dict' >> beam.Map(lambda fields: {
            'user_id': fields[0],
            'product_id': fields[1],
            'category': fields[2], 
            'amount': fields[3], 
            'timestamp': fields[4]
        })
        | 'Write To BigQuery' >> beam.io.WriteToBigQuery(
    table=args.output_table, schema=schema,
    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
        )
    )
    return p.run()


if __name__ == '__main__':
    try: 
        run()
        print("\n PIPELINE COMPLETED \n")
    except (KeyboardInterrupt, SystemExit):
        raise
    except:
        print("\n PIPELINE FAILED")
        traceback.print_exc()

