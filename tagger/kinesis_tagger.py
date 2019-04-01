import botocore

from tagger.base_tagger import is_retryable_exception, _arn_to_name, format_dict, client
from retrying import retry

class KinesisTagger(object):
    def __init__(self, dryrun, verbose, role=None, region=None):
        self.dryrun = dryrun
        self.verbose = verbose
        self.kinesis = client('kinesis', role=role, region=region)

    def tag(self, resource_arn, tags):
        if self.verbose:
            print("tagging %s with %s" % (resource_arn, format_dict(tags)))
        if not self.dryrun:
            try:
                stream_name = _arn_to_name(resource_arn)
                self._kinesis_add_tags_to_stream(StreamName=stream_name, Tags=tags)
            except botocore.exceptions.ClientError as exception:
                if exception.response["Error"]["Code"] in ['ResourceNotFoundException']:
                    print("Resource not found: %s" % resource_arn)
                else:
                    raise exception

    @retry(retry_on_exception=is_retryable_exception, stop_max_delay=30000, wait_exponential_multiplier=1000)
    def _kinesis_add_tags_to_stream(self, **kwargs):
        return self.kinesis.add_tags_to_stream(**kwargs)