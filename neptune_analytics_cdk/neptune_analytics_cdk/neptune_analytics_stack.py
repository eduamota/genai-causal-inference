from aws_cdk import (
    Stack,
    aws_ec2 as ec2,
    aws_neptunegraph as neptunegraph,
    aws_s3 as s3,
    aws_iam as iam,
    CfnOutput,
    RemovalPolicy,
    CfnResource,
)
from constructs import Construct
from .bedrock_agent_construct import BedrockAgentConstruct

class NeptuneAnalyticsStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Create an S3 bucket for Neptune Analytics data
        data_bucket = s3.Bucket(
            self, "NeptuneAnalyticsDataBucket",
            bucket_name=f"neptune-analytics-data-{self.account}-{self.region}",
            removal_policy=RemovalPolicy.DESTROY,  # For easy cleanup
            auto_delete_objects=True,  # For easy cleanup
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.S3_MANAGED,
            versioned=False,
        )

        # Create a Neptune Analytics Graph
        graph = neptunegraph.CfnGraph(
            self, "NeptuneAnalyticsGraph",
            graph_name="causal-analysis-graph",
            provisioned_memory=16,  # 16 GB as requested
            replica_count=0,
            vector_search_configuration=neptunegraph.CfnGraph.VectorSearchConfigurationProperty(
                vector_search_dimension=1024
            ),
            public_connectivity=True,  # Enable public access as requested
        )

        # Create an IAM role for Neptune Analytics to access S3
        neptune_s3_role = iam.Role(
            self, "NeptuneAnalyticsS3Role",
            assumed_by=iam.ServicePrincipal("neptune-graph.amazonaws.com"),
            description="Role for Neptune Analytics to access S3 data"
        )

        # Grant the role read access to the S3 bucket
        data_bucket.grant_read(neptune_s3_role)
        
        # Create Bedrock agent for Neptune Analytics
        bedrock_agent = BedrockAgentConstruct(
            self, "BedrockAgent",
            neptune_analytics_endpoint=graph.attr_graph_id
        )

        # Output the Neptune Analytics endpoint
        CfnOutput(
            self, "NeptuneAnalyticsEndpoint",
            value=graph.attr_graph_id,
            description="Neptune Analytics Graph Endpoint"
        )

        # Output the S3 bucket name
        CfnOutput(
            self, "NeptuneAnalyticsDataBucketName",
            value=data_bucket.bucket_name,
            description="S3 Bucket for Neptune Analytics Data"
        )

        # Output the IAM role ARN
        CfnOutput(
            self, "NeptuneAnalyticsS3RoleArn",
            value=neptune_s3_role.role_arn,
            description="IAM Role ARN for Neptune Analytics S3 Access"
        )
