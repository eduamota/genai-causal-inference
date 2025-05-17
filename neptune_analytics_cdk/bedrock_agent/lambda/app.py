import json
import boto3
import os
from aws_lambda_powertools import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext
from aws_bedrock.agents import AgentResolverBase, ActionGroupInvocationResult

logger = Logger()

class NeptuneAnalyticsResolver(AgentResolverBase):
    def __init__(self):
        self.neptune_endpoint = os.environ.get('NEPTUNE_ANALYTICS_ENDPOINT')
        self.region = os.environ.get('AWS_REGION', 'us-east-1')
        self.neptune_client = boto3.client('neptune-graph', region_name=self.region)
    
    def execute_query(self, query, params=None):
        """Execute an openCypher query against Neptune Analytics"""
        try:
            if params:
                response = self.neptune_client.execute_query(
                    graphIdentifier=self.neptune_endpoint,
                    queryString=query,
                    language='OPEN_CYPHER',
                    parameters=params
                )
            else:
                response = self.neptune_client.execute_query(
                    graphIdentifier=self.neptune_endpoint,
                    queryString=query,
                    language='OPEN_CYPHER'
                )
            
            # Parse the response
            payload = json.loads(response['payload'].read())
            return payload
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            return {"error": str(e)}

    def get_rating_statistics(self, action_group, parameters, request_body):
        """Get statistics about ratings"""
        query = """
        MATCH (r:rating)
        RETURN r.rating_numeric, count(*) as count
        """
        result = self.execute_query(query)
        return ActionGroupInvocationResult(
            action_group=action_group,
            response={"statistics": result}
        )
    
    def get_resolution_impact(self, action_group, parameters, request_body):
        """Get the impact of resolution on ratings"""
        query = """
        MATCH (res:resolution)-[r:AFFECTS]->(rating:rating)
        RETURN res.resolution_status, 
               avg(rating.rating_numeric) AS avg_rating,
               count(*) AS count
        """
        result = self.execute_query(query)
        return ActionGroupInvocationResult(
            action_group=action_group,
            response={"impact_data": result}
        )
    
    def get_understanding_impact(self, action_group, parameters, request_body):
        """Get the impact of understanding on ratings"""
        query = """
        MATCH (u:understanding)-[r:INFLUENCES]->(rating:rating)
        RETURN u.understanding_status, 
               avg(rating.rating_numeric) AS avg_rating,
               count(*) AS count
        """
        result = self.execute_query(query)
        return ActionGroupInvocationResult(
            action_group=action_group,
            response={"impact_data": result}
        )
    
    def get_combined_factors(self, action_group, parameters, request_body):
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
        result = self.execute_query(query)
        return ActionGroupInvocationResult(
            action_group=action_group,
            response={"combined_data": result}
        )
    
    def get_platform_comparison(self, action_group, parameters, request_body):
        """Compare AWS vs non-AWS platforms"""
        query = """
        MATCH (t:ticket)-[:HAS_RATING]->(rating:rating)
        RETURN t.aws_platform_numeric, 
               avg(rating.rating_numeric) AS avg_rating,
               count(*) AS count
        """
        result = self.execute_query(query)
        return ActionGroupInvocationResult(
            action_group=action_group,
            response={"platform_data": result}
        )
    
    def get_anomalies(self, action_group, parameters, request_body):
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
            return ActionGroupInvocationResult(
                action_group=action_group,
                response={"error": f"Unknown anomaly type: {anomaly_type}"}
            )
        
        result = self.execute_query(query)
        return ActionGroupInvocationResult(
            action_group=action_group,
            response={"anomalies": result}
        )
    
    def run_custom_query(self, action_group, parameters, request_body):
        """Run a custom openCypher query"""
        query = parameters.get("query")
        if not query:
            return ActionGroupInvocationResult(
                action_group=action_group,
                response={"error": "No query provided"}
            )
        
        # For security, we could add validation here
        result = self.execute_query(query)
        return ActionGroupInvocationResult(
            action_group=action_group,
            response={"query_result": result}
        )

# Initialize the resolver
resolver = NeptuneAnalyticsResolver()

# Lambda handler
@logger.inject_lambda_context
def lambda_handler(event, context: LambdaContext):
    return resolver.resolve(event, context)
