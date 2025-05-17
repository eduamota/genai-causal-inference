import json
import boto3
import os
import logging
import boto3
import traceback
from botocore.exceptions import ClientError

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

neptune_endpoint = os.environ.get('NEPTUNE_ANALYTICS_ENDPOINT')
region = os.environ.get('AWS_REGION', 'us-east-1')
neptune_client = boto3.client('neptune-graph', region_name=region)
    
def execute_query(query, params=None):
    """Execute an openCypher query against Neptune Analytics"""
    try:
        if params:
            response = neptune_client.execute_query(
                graphIdentifier=neptune_endpoint,
                queryString=query,
                language='OPEN_CYPHER',
                parameters=params
            )
        else:
            response = neptune_client.execute_query(
                graphIdentifier=neptune_endpoint,
                queryString=query,
                language='OPEN_CYPHER'
            )
        
        # Parse the response
        payload = json.loads(response['payload'].read().decode('utf-8'))
        return payload
    except Exception as e:
        logger.error(f"Error executing query: {e}")
        return {"error": str(e)}

def get_rating_statistics():
    """Get statistics about ratings"""
    query = """
    MATCH (r:rating)
    RETURN r.rating_numeric, count(*) as count
    """
    result = execute_query(query)
    return result

def get_resolution_impact():
    """Get the impact of resolution on ratings"""
    query = """
    MATCH (res:resolution)-[r:AFFECTS]->(rating:rating)
    RETURN res.resolution_status, 
            avg(rating.rating_numeric) AS avg_rating,
            count(*) AS count
    """
    result = execute_query(query)
    return result

def get_understanding_impact():
    """Get the impact of understanding on ratings"""
    query = """
    MATCH (u:understanding)-[r:INFLUENCES]->(rating:rating)
    RETURN u.understanding_status, 
            avg(rating.rating_numeric) AS avg_rating,
            count(*) AS count
    """
    result = execute_query(query)
    return result

def get_combined_factors():
    """Get the combined effect of resolution and understanding"""
    query = """
    MATCH (res:resolution)-[:AFFECTS]->(rating:rating),
            (u:understanding)-[:INFLUENCES]->(rating)
    RETURN res.resolution_status, 
            u.understanding_status,
            avg(rating.rating_numeric) AS avg_rating,
            count(*) AS count
    ORDER BY avg_rating DESC
    """
    result = execute_query(query)
    return result

def get_platform_comparison():
    """Compare AWS vs non-AWS platforms"""
    query = """
    MATCH (t:ticket)-[:HAS_RATING]->(rating:rating)
    RETURN t.aws_platform_numeric, 
            avg(rating.rating_numeric) AS avg_rating,
            count(*) AS count
    """
    result = execute_query(query)
    return result

def get_anomalies(parameters):
    """Find anomalies in the data"""
    anomaly_type = parameters.get("anomaly_type", "sentiment_rating_mismatch")
    
    if anomaly_type == "sentiment_rating_mismatch":
        query = """
        MATCH (t:ticket)-[:HAS_RATING]->(rating:rating)
        WHERE (t.sentiment_numeric = 1 AND rating.rating_numeric = 0) OR
                (t.sentiment_numeric = 0 AND rating.rating_numeric = 1)
        RETURN t.custom_platform, t.resolution_status, t.understanding_status,
                t.sentiment_numeric, rating.rating_numeric
        LIMIT 10
        """
    elif anomaly_type == "extreme_effects":
        query = """
        MATCH (t:ticket)
        RETURN t.custom_platform, t.resolution_status, t.understanding_status, 
                t.resolution_effect, t.rating_numeric
        ORDER BY abs(t.resolution_effect) DESC
        LIMIT 10
        """
    else:
        return {"error": f"Unknown anomaly type: {anomaly_type}"}
    
    result = execute_query(query)
    return result

def run_custom_query(parameters):
    """Run a custom openCypher query"""
    query = parameters.get("query")
    if not query:
        return {"error": "No query provided"}
    
    # For security, we could add validation here
    result = execute_query(query)
    return result

def handler(event, context):
    try:
        
        action = event['actionGroup']
        api_path = event['apiPath']
        logger.info(f"Processing action: {action}, API path: {api_path}")

        if api_path == '/getRatingStatistics':
            body = get_rating_statistics()
        elif api_path == '/getResolutionImpact':
            body = get_resolution_impact()
        elif api_path == '/getUnderstandingImpact':
            body = get_understanding_impact()
        elif api_path == '/getCombinedFactors':
            body = get_combined_factors()
        elif api_path == '/getPlatformComparison':
            body = get_platform_comparison()
        elif api_path == '/getAnomalies':
            parameters = event.get('parameters', [])
            body = get_anomalies(parameters)
        elif api_path == '/runCustomQuery':
            parameters = event.get('parameters', [])
            body = run_custom_query(parameters)
        else:
            logger.warning(f"Invalid API path: {api_path}")
            body = {"{}::{} is not a valid api, try another one.".format(action, api_path)}

        response_body = {
            'application/json': {
                'body': str(body)
            }
        }

        action_response = {
            'actionGroup': event['actionGroup'],
            'apiPath': event['apiPath'],
            'httpMethod': event['httpMethod'],
            'httpStatusCode': 200,
            'responseBody': response_body
        }

        response = {'response': action_response}
        logger.info("Lambda execution completed successfully")
        return response
        
    except Exception as e:
        logger.error(f"Unhandled exception in Lambda handler: {str(e)}")
        logger.error(traceback.format_exc())
        
        # Return a proper error response
        error_response = {
            'response': {
                'actionGroup': event.get('actionGroup', 'unknown'),
                'apiPath': event.get('apiPath', 'unknown'),
                'httpMethod': event.get('httpMethod', 'unknown'),
                'httpStatusCode': 500,
                'responseBody': {
                    'application/json': {
                        'body': json.dumps({
                            'error': f"Lambda execution failed: {str(e)}"
                        })
                    }
                }
            }
        }
        
        return error_response
