from aws_cdk import (
    aws_lambda as lambda_,
    aws_iam as iam,
    CfnOutput,
    Duration,
    Stack,
    aws_bedrock as bedrock,
)
from constructs import Construct
import json
import os

class BedrockAgentConstruct(Construct):
    def __init__(self, scope: Construct, id: str, neptune_analytics_endpoint: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        # Step 1: Create Lambda function for the agent
        agent_lambda = lambda_.Function(
            self, "NeptuneAnalyticsAgentLambda",
            runtime=lambda_.Runtime.PYTHON_3_10,
            code=lambda_.Code.from_asset("bedrock_agent/lambda"),
            handler="app.lambda_handler",
            timeout=Duration.seconds(30),
            memory_size=256,
            environment={
                "NEPTUNE_ANALYTICS_ENDPOINT": neptune_analytics_endpoint,
            }
        )
        
        # Add permissions to query Neptune Analytics
        agent_lambda.add_to_role_policy(
            iam.PolicyStatement(
                actions=["neptune-graph:ExecuteQuery",
                "neptune-graph:DeleteDataViaQuery", 
                "neptune-graph:ReadDataViaQuery", 
                "neptune-graph:DescribeGraph",
                "neptune-graph:WriteDataViaQuery"],
                resources=[f"arn:aws:neptune-graph:{Stack.of(self).region}:{Stack.of(self).account}:graph/{neptune_analytics_endpoint}"]
            )
        )

        agent_lambda.add_permission(
            id="AllowBedrockInvoke",
            principal=iam.ServicePrincipal("bedrock.amazonaws.com"),
            action="lambda:InvokeFunction",
            source_arn=f"arn:aws:bedrock:{Stack.of(self).region}:{Stack.of(self).account}:agent:*"
        )
        
        # Create agent role
        agent_role = iam.Role(
            self, "BedrockAgentRole",
            assumed_by=iam.ServicePrincipal("bedrock.amazonaws.com")
        )
        
        # Add permissions to invoke Lambda
        agent_role.add_to_policy(
            iam.PolicyStatement(
                actions=["lambda:InvokeFunction"],
                resources=[agent_lambda.function_arn]
            )
        )

        # Add permissions to bedrock actions
        agent_role.add_to_policy(
            iam.PolicyStatement(
                actions=["bedrock:*"],
                resources=["*"]
            )
        )
        
        # Load the OpenAPI schema from file
        openapi_schema_path = os.path.join("bedrock_agent", "openapi.json")
        with open(openapi_schema_path, 'r') as f:
            openapi_schema = json.load(f)
        
        # Step 2: Create Bedrock agent with inline action group and inline OpenAPI schema
        agent = bedrock.CfnAgent(
            self, "NeptuneAnalyticsAgent",
            agent_name="NeptuneAnalyticsSupportAgent",
            agent_resource_role_arn=agent_role.role_arn,
            foundation_model="anthropic.claude-3-sonnet-20240229-v1:0",
            instruction="""
            You are a technical specialist agent that helps human support agents improve their job performance by analyzing Neptune Analytics data. 
            Your goal is to provide insights from the causal analysis data to help agents understand what factors lead to better customer ratings.
            
            Key insights to provide:
            1. The impact of issue resolution on customer ratings
            2. The impact of agent understanding on customer ratings
            3. The combined effect of resolution and understanding
            4. Differences between AWS and non-AWS platforms
            5. Anomalies in the data that might need special attention
            
            Be professional, concise, and data-driven in your responses. Provide specific, actionable recommendations based on the data.
            """,
            idle_session_ttl_in_seconds=1800,  # 30 minutes
            
            # Define action groups inline as part of the agent definition
            action_groups=[
                bedrock.CfnAgent.AgentActionGroupProperty(
                    action_group_name="NeptuneAnalyticsActions",
                    description="Actions for querying Neptune Analytics data",
                    action_group_executor=bedrock.CfnAgent.ActionGroupExecutorProperty(
                        lambda_=agent_lambda.function_arn
                    ),
                    api_schema=bedrock.CfnAgent.APISchemaProperty(
                        payload=json.dumps(openapi_schema)  # Use inline OpenAPI schema
                    )
                )
            ]
        )
        
        # Add explicit dependency to ensure proper creation order
        agent.node.add_dependency(agent_lambda)

        
        # Outputs
        CfnOutput(
            self, "AgentId",
            value=agent.ref,
            description="Bedrock Agent ID"
        )
        
        CfnOutput(
            self, "AgentLambdaArn",
            value=agent_lambda.function_arn,
            description="Agent Lambda Function ARN"
        )
