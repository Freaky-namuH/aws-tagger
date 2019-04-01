import botocore

from tagger.base_tagger import is_retryable_exception, format_dict, client
from retrying import retry

class LambdaTagger(object):
    def __init__(self, dryrun, verbose, role=None, region=None):
        self.dryrun = dryrun
        self.verbose = verbose
        self.alambda = client('lambda', role=role, region=region)

    def tag(self, resource_arn, tags):
        if self.verbose:
            print("tagging %s with %s" % (resource_arn, format_dict(tags)))
        if not self.dryrun:
            try:
                self._lambda_tag_resource(Resource=resource_arn, Tags=tags)
            except botocore.exceptions.ClientError as exception:
                if exception.response["Error"]["Code"] in ['ResourceNotFoundException']:
                    print("Resource not found: %s" % resource_arn)
                else:
                    raise exception

    @retry(retry_on_exception=is_retryable_exception, stop_max_delay=30000, wait_exponential_multiplier=1000)
    def _lambda_tag_resource(self, **kwargs):
        return self.alambda.tag_resource(**kwargs)