"""A streaming word-counting workflow.
"""

from __future__ import absolute_import

import logging
import sys

from apache_beam import pvalue
from apache_beam.runners.portability import portable_runner
from apache_beam.transforms.trigger import AfterProcessingTime, AccumulationMode, Repeatedly

import apache_beam as beam
import apache_beam.transforms.window as window
from apache_beam.options.pipeline_options import PipelineOptions


class TestInput(beam.PTransform):
    def expand(self, pbegin):
        assert isinstance(pbegin, pvalue.PBegin), (
                'Input to transform must be a PBegin but found %s' % pbegin)
        return pvalue.PCollection(pbegin.pipeline)

    def get_windowing(self, inputs):
        return beam.Windowing(window.GlobalWindows())

    def infer_output_type(self, unused_input_type):
        return bytes

    def to_runner_api_parameter(self, context):
        assert isinstance(self, TestInput), \
            "expected instance of FlinkKinesisInput, but got %s" % self.__class__
        return ("mwylde:testStreamingInput", "")

    @staticmethod
    @beam.PTransform.register_urn("mwylde:testStreamingInput", None)
    def from_runner_api_parameter(spec_parameter, unused_context):
        print "spec: " + spec_parameter
        instance = TestInput()
        return instance


def split(s):
    a = s.split("-")
    return a[0], int(a[1])


def find_largest(x):
    return x[0], max(x[1])

def apply_timestamp(element):
    import time
    import apache_beam.transforms.window as window
    yield window.TimestampedValue(element, time.time())

def run(argv=None):
    """Build and run the pipeline."""
    runner = portable_runner.PortableRunner()
    options_string = sys.argv.extend([
      "--experiments=beam_fn_api",
      "--sdk_location=container",
      "--job_endpoint=localhost:8099",
      "--streaming"
    ])

    pipeline_options = PipelineOptions(options_string)

    p = beam.Pipeline(runner=runner, options=pipeline_options)

    messages = (p | TestInput())

    (messages | 'decode' >> beam.Map(lambda x: x.decode('utf-8'))
              # | 'window' >> beam.WindowInto(window.GlobalWindows(),
              #                               trigger=Repeatedly(AfterProcessingTime(15 * 1000)),
              #                               accumulation_mode=AccumulationMode.DISCARDING)
              | 'timestamp' >> beam.FlatMap(apply_timestamp)
              | 'window' >> beam.WindowInto(window.FixedWindows(15),
                                            trigger=AfterProcessingTime(20 * 1000),
                                            accumulation_mode=AccumulationMode.DISCARDING)
              | 'split' >> beam.Map(split)
              | 'group' >> beam.GroupByKey()
              | 'count' >> beam.Map(find_largest)
              | 'log' >> beam.Map(lambda x: logging.info("%s: %d" % x)))

    result = p.run()
    result.wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
