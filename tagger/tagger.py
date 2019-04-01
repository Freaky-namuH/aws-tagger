import csv

from botocore.exceptions import ClientError
from .cloudfront_tagger import CloudfrontTagger
from .cloudwatch_logs_tagger import CloudWatchLogsTagger
from .dynamodb_tagger import DynamoDBTagger
from .ec2_tagger import EC2Tagger
from .efs_tagger import EFSTagger
from .elasticache_tagger import ElasticacheTagger
from .elasticsearch_tagger import ESTagger
from .kinesis_tagger import KinesisTagger
from .lambda_tagger import LambdaTagger
from .loadbalancer_tagger import LBTagger
from .rds_tagger import RDSTagger
from .s3_tagger import S3Tagger
from .acm_pca_tagger import ACMPCATagger


def parse_arn(arn):
    # http://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html
    elements = arn.split(':', 5)
    result = {
        'arn': elements[0],
        'partition': elements[1],
        'service': elements[2],
        'region': elements[3],
        'account': elements[4],
        'resource': elements[5],
        'resource_type': None
    }
    if '/' in result['resource']:
        result['resource_type'], result['resource'] = result['resource'].split('/', 1)
    elif ':' in result['resource']:
        result['resource_type'], result['resource'] = result['resource'].split(':', 1)
    return result


class SingleResourceTagger(object):
    def __init__(self, dryrun, verbose, role=None, region=None, tag_volumes=False):
        self.taggers = {}
        self.taggers['ec2'] = EC2Tagger(dryrun, verbose, role=role, region=region, tag_volumes=tag_volumes)
        self.taggers['elasticfilesystem'] = EFSTagger(dryrun, verbose, role=role, region=region)
        self.taggers['rds'] = RDSTagger(dryrun, verbose, role=role, region=region)
        self.taggers['elasticloadbalancing'] = LBTagger(dryrun, verbose, role=role, region=region)
        self.taggers['elasticache'] = ElasticacheTagger(dryrun, verbose, role=role, region=region)
        self.taggers['s3'] = S3Tagger(dryrun, verbose, role=role, region=region)
        self.taggers['es'] = ESTagger(dryrun, verbose, role=role, region=region)
        self.taggers['kinesis'] = KinesisTagger(dryrun, verbose, role=role, region=region)
        self.taggers['cloudfront'] = CloudfrontTagger(dryrun, verbose, role=role, region=region)
        self.taggers['logs'] = CloudWatchLogsTagger(dryrun, verbose, role=role, region=region)
        self.taggers['dynamodb'] = DynamoDBTagger(dryrun, verbose, role=role, region=region)
        self.taggers['lambda'] = LambdaTagger(dryrun, verbose, role=role, region=region)
        self.taggers['acm-pca'] = ACMPCATagger(dryrun, verbose, role=role, region=region)

    def tag(self, resource_id, tags):
        if resource_id == "":
            return

        if len(tags) == 0:
            return

        tagger = None
        resource_arn = resource_id
        if resource_id.startswith('arn:'):
            product, resource_id = self._parse_arn(resource_id)
            if product:
                tagger = self.taggers.get(product)
        else:
            tagger = self.taggers['s3']

        if resource_id.startswith('i-'):
            tagger = self.taggers['ec2']
            resource_arn = resource_id
        elif resource_id.startswith('vol-'):
            tagger = self.taggers['ec2']
            resource_arn = resource_id
        elif resource_id.startswith('nat-'):
            tagger = self.taggers['ec2']
            resource_arn = resource_id
        elif resource_id.startswith('vpn-'):
            tagger = self.taggers['ec2']
            resource_arn = resource_id
        elif resource_id.startswith('cgw-'):
            tagger = self.taggers['ec2']
            resource_arn = resource_id
        elif resource_id.startswith('vgw-'):
            tagger = self.taggers['ec2']
            resource_arn = resource_id

        elif resource_id.startswith('snap-'):
            tagger = self.taggers['ec2']
            resource_arn = resource_id

        try:
            if tagger:
                tagger.tag(resource_arn, tags)
            else:
                print("Tagging is not support for this resource %s" % resource_id)
        except ClientError as e:
            print("Failed to apply tags to {0}: {1}".format(resource_arn, e))
            return False

        return True

    def _parse_arn(self, resource_arn):
        product = None
        resource_id = None
        parts = resource_arn.split(':')
        if len(parts) > 5:
            product = parts[2]
            resource_id = parts[5]
            resource_parts = resource_id.split('/')
            if len(resource_parts) > 1:
                resource_id = resource_parts[-1]

        return product, resource_id


class MultipleResourceTagger(object):
    def __init__(self, dryrun, verbose, role=None, region=None, tag_volumes=False):
        self.dryrun = dryrun
        self.verbose = verbose
        self.tag_volumes = tag_volumes
        self.role = role
        self.region = region
        self.regional_tagger = {}

    def tag(self, resource_ids, tags):
        for resource_id in resource_ids:
            if resource_id.startswith('arn:'):
                arn = parse_arn(resource_id)
                region = arn['region']

            tagger = self.regional_tagger.get(region)
            if tagger is None:
                tagger = SingleResourceTagger(self.dryrun, self.verbose, role=self.role, region=region,
                                              tag_volumes=self.tag_volumes)
                self.regional_tagger[region] = tagger

            tagger.tag(resource_id, tags)


class CSVResourceTagger(object):
    def __init__(self, dryrun, verbose, role=None, region=None, tag_volumes=False):
        self.dryrun = dryrun
        self.verbose = verbose
        self.tag_volumes = tag_volumes
        self.role = role
        self.region = region
        self.regional_tagger = {}
        self.resource_id_column = 'Id'
        self.region_column = 'Region'

    def tag(self, filename):
        with open(filename, 'rU') as csv_file:
            reader = csv.reader(csv_file)
            header_row = True
            tag_index = None

            for row in reader:
                if header_row:
                    header_row = False
                    tag_index = self._parse_header(row)
                else:
                    self._tag_resource(tag_index, row)

    def _parse_header(self, header_row):
        tag_index = {}
        for index, name in enumerate(header_row):
            tag_index[name] = index

        return tag_index

    def _tag_resource(self, tag_index, row):
        resource_id = row[tag_index[self.resource_id_column]]
        tags = {}
        for (key, index) in tag_index.items():
            value = row[index]
            if key != self.resource_id_column and key != self.region_column and value != "":
                tags[key] = value

        tagger = self._lookup_tagger(resource_id, tag_index, row)
        print("Applying tags to {0}",format(resource_id))
        if tagger.tag(resource_id, tags):
            print("Successfully applied tags to {0}".format(resource_id))

    def _lookup_tagger(self, resource_id, tag_index, row):
        region = self.region
        region_index = tag_index.get(self.region_column)

        if region_index is not None:
            region = row[region_index]
        elif resource_id.startswith('arn:'):
            arn = parse_arn(resource_id)
            region = arn['region']

        if region == '':
            region = None

        tagger = self.regional_tagger.get(region)
        if tagger is None:
            tagger = SingleResourceTagger(self.dryrun, self.verbose, role=self.role, region=region,
                                          tag_volumes=self.tag_volumes)
            self.regional_tagger[region] = tagger

        return tagger
