import boto3
import cfnresponse
import json

def handler(event, context):
    print(f"Event: {json.dumps(event)}")
    
    response_data = {}
    physical_id = event.get('PhysicalResourceId', None)
    
    try:
        request_type = event['RequestType']
        resource_type = event['ResourceProperties'].get('ResourceType')
        
        if request_type == 'Create':
            if resource_type == 'Agent':
                # Create Bedrock agent
                bedrock_client = boto3.client('bedrock-agent')
                agent_response = bedrock_client.create_agent(
                    agentName=event['ResourceProperties'].get('AgentName'),
                    agentResourceRoleArn=event['ResourceProperties'].get('AgentResourceRoleArn'),
                    foundationModel=event['ResourceProperties'].get('FoundationModel'),
                    instruction=event['ResourceProperties'].get('Instruction'),
                    idleSessionTTLInSeconds=int(event['ResourceProperties'].get('IdleSessionTTLInSeconds', 1800))
                )
                physical_id = agent_response['agentId']
                response_data['AgentId'] = agent_response['agentId']
                
            elif resource_type == 'ActionGroup':
                # Create Bedrock agent action group
                bedrock_client = boto3.client('bedrock-agent')
                action_group_response = bedrock_client.create_agent_action_group(
                    agentId=event['ResourceProperties'].get('AgentId'),
                    actionGroupName=event['ResourceProperties'].get('ActionGroupName'),
                    actionGroupExecutor={
                        'lambda': {
                            'lambdaArn': event['ResourceProperties'].get('LambdaArn')
                        }
                    },
                    apiSchema={
                        'payload': {
                            's3': {
                                's3BucketName': event['ResourceProperties'].get('S3BucketName'),
                                's3ObjectKey': event['ResourceProperties'].get('S3ObjectKey')
                            }
                        },
                        'type': 'OPEN_API'
                    },
                    description=event['ResourceProperties'].get('Description')
                )
                physical_id = action_group_response['actionGroupId']
                response_data['ActionGroupId'] = action_group_response['actionGroupId']
        
        elif request_type == 'Update':
            # Handle updates if needed
            pass
            
        elif request_type == 'Delete':
            if resource_type == 'ActionGroup' and physical_id:
                # Delete action group
                bedrock_client = boto3.client('bedrock-agent')
                bedrock_client.delete_agent_action_group(
                    agentId=event['ResourceProperties'].get('AgentId'),
                    actionGroupId=physical_id
                )
                
            elif resource_type == 'Agent' and physical_id:
                # Delete agent
                bedrock_client = boto3.client('bedrock-agent')
                bedrock_client.delete_agent(agentId=physical_id)
        
        cfnresponse.send(event, context, cfnresponse.SUCCESS, response_data, physical_id)
    except Exception as e:
        print(f"Error: {str(e)}")
        cfnresponse.send(event, context, cfnresponse.FAILED, {"Error": str(e)}, physical_id)
