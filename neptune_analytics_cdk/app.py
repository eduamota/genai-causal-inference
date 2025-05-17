#!/usr/bin/env python3
import os
import aws_cdk as cdk
from neptune_analytics_cdk.neptune_analytics_stack import NeptuneAnalyticsStack

app = cdk.App()
NeptuneAnalyticsStack(app, "NeptuneAnalyticsStack",
    env=cdk.Environment(
        account=os.environ.get("CDK_DEFAULT_ACCOUNT"),
        region=os.environ.get("CDK_DEFAULT_REGION")
    )
)

app.synth()
