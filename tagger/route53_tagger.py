import botocore

from tagger.base_tagger import is_retryable_exception, format_dict, dict_to_aws_tags, client
from retrying import retry
from .utils import parse_arn

class Route53Tagger(object):
    def __init__(self, dryrun, verbose, role=None, region=None):
        self.dryrun = dryrun
        self.verbose = verbose
        self.route53 = client('route53', role=role, region=region)

    def tag(self, resource_arn, tags):
        aws_tags = dict_to_aws_tags(tags)
        if self.verbose:
            print("tagging %s with %s" % (resource_arn, format_dict(tags)))
        if not self.dryrun:
            try:
                arn = parse_arn(resource_arn)

                self._route53_add_tags(ResourceType=arn['resource_type'], ResourceId=arn['resource'], AddTags=aws_tags)
            except botocore.exceptions.ClientError as exception:
                if exception.response["Error"]["Code"] in ['ValidationException']:
                    print("Resource not found: %s" % resource_arn)
                else:
                    raise exception

    @retry(retry_on_exception=is_retryable_exception, stop_max_delay=30000, wait_exponential_multiplier=1000)
    def _route53_add_tags(self, **kwargs):
        return self.route53.change_tags_for_resource(**kwargs)