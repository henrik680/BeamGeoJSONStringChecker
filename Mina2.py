from sys import argv
import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import argparse




class custom_json_parser2(beam.DoFn):
    def process(self, element):
        #norm = json_normalize(element, max_level=1)
        #l = norm["features"].to_list()

        l = element["features"]
        #geo_dict = ast.literal_eval(element)
        #l = geo_dict["features"]
        #logging.info("l={}".format(l))
        l_new = []
        for x in l:
            l_new.append(str(x))
            #l_new.append("hej")
        #print(l_new)
        #return {'geometry': l_new}
        return l_new


class custom_json_parser(beam.DoFn):
    def process(self, element):
        l_new = []
        for x in element["features"]:
            properties = x['properties']
            geo = {
                'geometry': str(x['geometry'])
            }
            d = {**properties, **geo}
            l_new.append(d)
        return l_new


parser = argparse.ArgumentParser()
parser.add_argument(
    '--input',
    dest='input',
    default='gs://dataflow-sample',
    help='Input file to process.')
known_args, pipeline_args = parser.parse_known_args(argv)
p = beam.Pipeline(options=PipelineOptions(pipeline_args))

(p

     #| 'Read from a File' >> beam.io.ReadFromText(known_args.input, skip_header_lines=1)
     | 'Read from a File' >> beam.io.ReadFromText(known_args.input)
     | 'Load JSON' >> beam.Map(json.loads)
     | "CUSTOM PARSE" >> beam.ParDo(custom_json_parser())
     #| 'To string' >> beam.ToString.Element()
     | beam.Map(print))
p.run().wait_until_finish()
