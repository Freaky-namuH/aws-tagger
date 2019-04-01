import botocore

from tagger.base_tagger import is_retryable_exception, format_dict, dict_to_aws_tags, client
from retrying import retry

class DynamoDBTagger(object):
    def __init__(self, dryrun, verbose, role=None, region=None):
        self.dryrun = dryrun
        self.verbose = verbose
        self.dynamodb = client('dynamodb', role=role, region=region)

    def tag(self, resource_arn, tags):
        aws_tags = dict_to_aws_tags(tags)
        if self.verbose:
            print("tagging %s with %s" % (resource_arn, format_dict(tags)))
        if not self.dryrun:
            try:
                self._dynamodb_tag_resource(ResourceArn=resource_arn, Tags=aws_tags)
            except botocore.exceptions.ClientError as exception:
                if exception.response["Error"]["Code"] in ['ResourceNotFoundException']:
                    print("Resource not found: %s" % resource_arn)
                else:
                    raise exception

    @retry(retry_on_exception=is_retryable_exception, stop_max_delay=30000, wait_exponential_multiplier=1000)
    def _dynamodb_tag_resource(self, **kwargs):
        return self.dynamodb.tag_resource(**kwargs)