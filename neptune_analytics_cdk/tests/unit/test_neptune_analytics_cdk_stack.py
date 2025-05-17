import aws_cdk as core
import aws_cdk.assertions as assertions

from neptune_analytics_cdk.neptune_analytics_cdk_stack import NeptuneAnalyticsCdkStack

# example tests. To run these tests, uncomment this file along with the example
# resource in neptune_analytics_cdk/neptune_analytics_cdk_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = NeptuneAnalyticsCdkStack(app, "neptune-analytics-cdk")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
