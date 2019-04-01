import botocore

from tagger.base_tagger import is_retryable_exception, format_dict, dict_to_aws_tags, client
from retrying import retry

class EC2Tagger(object):
    def __init__(self, dryrun, verbose, role=None, region=None, tag_volumes=False):
        self.dryrun = dryrun
        self.verbose = verbose
        self.ec2 = client('ec2', role=role, region=region)
        self.volume_cache = {}
        if tag_volumes:
            self.add_volume_cache()

    def add_volume_cache(self):
        #TODO implement paging for describe instances
        reservations = self._ec2_describe_instances(MaxResults=1000)

        for reservation in reservations["Reservations"]:
            for instance in reservation["Instances"]:
                instance_id = instance['InstanceId']
                volumes = instance.get('BlockDeviceMappings', [])
                self.volume_cache[instance_id] = []
                for volume in volumes:
                    ebs = volume.get('Ebs', {})
                    volume_id = ebs.get('VolumeId')
                    if volume_id:
                        self.volume_cache[instance_id].append(volume_id)

    def tag(self, instance_id, tags):
        aws_tags = dict_to_aws_tags(tags)
        resource_ids = [instance_id]
        resource_ids.extend(self.volume_cache.get(instance_id, []))
        if self.verbose:
            print("tagging %s with %s" % (", ".join(resource_ids), format_dict(tags)))
        if not self.dryrun:
            try:
                self._ec2_create_tags(Resources=resource_ids, Tags=aws_tags)
            except botocore.exceptions.ClientError as exception:
                if exception.response["Error"]["Code"] in ['InvalidSnapshot.NotFound', 'InvalidVolume.NotFound', 'InvalidInstanceID.NotFound']:
                    print("Resource not found: %s" % instance_id)
                else:
                    raise exception


    @retry(retry_on_exception=is_retryable_exception, stop_max_delay=30000, wait_exponential_multiplier=1000)
    def _ec2_describe_instances(self, **kwargs):
        return self.ec2.describe_instances(**kwargs)

    @retry(retry_on_exception=is_retryable_exception, stop_max_delay=30000, wait_exponential_multiplier=1000)
    def _ec2_create_tags(self, **kwargs):
        return self.ec2.create_tags(**kwargs)