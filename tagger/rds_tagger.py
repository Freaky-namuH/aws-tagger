import botocore

from tagger.base_tagger import is_retryable_exception, format_dict, dict_to_aws_tags, client
from retrying import retry

class RDSTagger(object):
    def __init__(self, dryrun, verbose, role=None, region=None):
        self.dryrun = dryrun
        self.verbose = verbose
        self.rds = client('rds', role=role, region=region)

    def tag(self, resource_arn, tags):
        aws_tags = dict_to_aws_tags(tags)
        if self.verbose:
            print("tagging %s with %s" % (resource_arn, format_dict(tags)))
        if not self.dryrun:
            try:
                self._rds_add_tags_to_resource(ResourceName=resource_arn, Tags=aws_tags)
            except botocore.exceptions.ClientError as exception:
                if exception.response["Error"]["Code"] in ['DBInstanceNotFound']:
                    print("Resource not found: %s" % resource_arn)
                else:
                    raise exception

    @retry(retry_on_exception=is_retryable_exception, stop_max_delay=30000, wait_exponential_multiplier=1000)
    def _rds_add_tags_to_resource(self, **kwargs):
        return self.rds.add_tags_to_resource(**kwargs)