from typing import Any, cast

from aws_cdk import (
    CfnOutput,
)
from aws_cdk import (
    aws_batch as batch,
)
from aws_cdk import (
    aws_ec2 as ec2,
)
from aws_cdk import (
    aws_iam as iam,
)
from constructs import Construct


class BatchInfra(Construct):
    """AWS Batch compute environment and queues."""

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        vpc: ec2.IVpc,
        max_vcpu: int,
        ami_id: str,
        stage: str,
        stack_name: str,
        **kwargs: Any,
    ) -> None:
        """Setup an AWS Batch ComputeEnvironment and JobQueue

        Parameters
        ----------
        vpc:
            VPC in which the ComputeEnvironment will launch Instances.
        max_vcpu:
            Maximum number of CPUs in the ComputeEnvironment
        ami_id:
            AWS Batch Ec2 instance AMI identifier, OR name of SSM parameter that
            references the AMI ID prefixed by `resolve:ssm` (e.g,
            `resolve:ssm:/param-name`).
        stage:
            Environment or "stage" for resources used to help distinguish resources
            from this stack.
        """
        super().__init__(scope, construct_id, **kwargs)

        # Note: AWS Batch requires a multi-part UserData to be provided as it will
        # add its own sections
        multipart_user_data = ec2.MultipartUserData(parts_separator="==BOUNDARY==")
        command_user_data = ec2.UserData.for_linux()
        command_user_data.add_commands(
            # https://docs.aws.amazon.com/AmazonECS/latest/developerguide/pull-behavior.html
            'echo "ECS_IMAGE_PULL_BEHAVIOR=prefer-cached" >> /etc/ecs/ecs.config',
        )
        multipart_user_data.add_part(
            ec2.MultipartBody.from_user_data(command_user_data)
        )

        if "resolve:ssm:" in ami_id:
            # https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/create-launch-template.html#use-an-ssm-parameter-instead-of-an-ami-id
            ec2_machine_image = ec2.MachineImage.resolve_ssm_parameter_at_launch(
                ami_id.removeprefix("resolve:ssm:")
            )
        else:
            ec2_machine_image = ec2.MachineImage.lookup(name=ami_id)
        ecs_machine_image = batch.EcsMachineImage(
            image=ec2_machine_image,
            image_type=batch.EcsMachineImageType.ECS_AL2,
        )
        launch_template = ec2.LaunchTemplate(
            self,
            "LaunchTemplate",
            machine_image=ec2_machine_image,
            user_data=multipart_user_data,
        )

        # Add role for Batch to assume that allows reading from SSM
        compute_environment_service_role = iam.Role(
            self,
            "ServiceRole",
            assumed_by=iam.ServicePrincipal("batch.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSBatchServiceRole"
                ),
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "AmazonSSMReadOnlyAccess"
                ),
            ],
        )

        self.compute_environment = batch.ManagedEc2EcsComputeEnvironment(
            self,
            f"CE-{stage.capitalize()}",
            allocation_strategy=batch.AllocationStrategy.SPOT_CAPACITY_OPTIMIZED,
            images=[ecs_machine_image],
            launch_template=launch_template,
            instance_classes=None,
            use_optimal_instance_classes=True,
            spot=True,
            minv_cpus=0,
            maxv_cpus=max_vcpu,
            vpc_subnets=ec2.SubnetSelection(
                subnet_type=ec2.SubnetType.PUBLIC,
            ),
            vpc=vpc,
            terminate_on_update=False,
            service_role=compute_environment_service_role,
            # Replacing compute environment allows update of more settings,
            # but means we cannot specify the compute env name.
            replace_compute_environment=True,
        )

        # ManagedEc2EcsComputeEnvironment requires an override to track `$Latest`
        # Ref: https://github.com/aws/aws-cdk/issues/28137
        cfn_ce = cast(
            batch.CfnComputeEnvironment,
            self.compute_environment.node.find_child("Resource"),
        )
        cfn_ce.add_property_override(
            "ComputeResources.LaunchTemplate.Version",
            launch_template.latest_version_number,
        )

        self.queue = batch.JobQueue(
            self,
            "JobQueue",
            job_queue_name=f"{stack_name}-job-queue",
        )
        self.queue.add_compute_environment(self.compute_environment, 1)

        CfnOutput(
            self,
            "JobQueueName",
            value=self.queue.job_queue_name,
        )
