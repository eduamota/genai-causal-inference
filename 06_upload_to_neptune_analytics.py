#!/usr/bin/env python3
"""
Script to upload data from neptune_export directory to Neptune Analytics graph.
This script handles:
1. Uploading CSV files directly to S3
2. Loading data from S3 into Neptune Analytics
"""

import os
import boto3
import argparse
import time
import logging
from botocore.exceptions import ClientError

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

class NeptuneAnalyticsClient:
    """Client for interacting with Neptune Analytics via openCypher"""
    
    def __init__(self, endpoint=None, port=8182):
        """
        Initialize the Neptune Analytics client
        
        Args:
            endpoint: The Neptune Analytics endpoint (hostname or IP)
            port: The port to connect to (default: 8182)
        """
        self.endpoint = endpoint
        self.port = port
        
        # Make sure we're using the VPC endpoint if we're in a VPC
        if self.endpoint and 'g-' in self.endpoint and '.neptune-graph.' in self.endpoint:
            # This looks like a public endpoint, check if we have a VPC endpoint ID
            vpc_endpoint_id = os.environ.get('NEPTUNE_VPC_ENDPOINT_ID')
            region = os.environ.get('AWS_REGION', 'us-west-2')
            
            if vpc_endpoint_id:
                # Make sure we don't have a double "vpce-" prefix
                vpc_id_clean = vpc_endpoint_id.replace('vpce-', '')
                vpc_endpoint_dns = f"vpce-{vpc_id_clean}.neptune-graph.{region}.vpce.amazonaws.com"
                logger.info(f"Replacing public endpoint {self.endpoint} with VPC endpoint {vpc_endpoint_dns}")
                self.endpoint = vpc_endpoint_dns
        
        logger.info(f"Initialized Neptune Analytics client with endpoint: {self.endpoint}:{self.port}")
    
    def execute_open_cypher(self, query, params=None):
        """
        Execute an openCypher query against Neptune Analytics
        
        Args:
            query: The openCypher query to execute
            params: Parameters for the query (default: None)
            
        Returns:
            The query results as a dictionary
        """
        if not self.endpoint:
            raise ValueError("Neptune Analytics endpoint not provided")
        

        client = boto3.client('neptune-graph')

        graphIdentifier = self.endpoint
        
        try:
            logger.info(f"Executing openCypher query against {graphIdentifier}")

            if params:
                # payload['parameters'] = params

                response = client.execute_query(
                    graphIdentifier=graphIdentifier,
                    queryString=query,
                    language='OPEN_CYPHER',
                    parameters=params
                )
            else: 

                response = client.execute_query(
                    graphIdentifier=graphIdentifier,
                    queryString=query,
                    language='OPEN_CYPHER'
                )
            
            return response['payload'].read()
                
        except Exception as e:
            logger.error(f"Error executing openCypher query: {e}")
            return None


def parse_args():
    parser = argparse.ArgumentParser(description='Upload data to Neptune Analytics')
    parser.add_argument('--endpoint', required=True, help='Neptune Analytics endpoint')
    parser.add_argument('--region', default='us-east-1', help='AWS region')
    parser.add_argument('--s3-bucket', required=True, help='S3 bucket name')
    parser.add_argument('--s3-prefix', default='neptune-analytics-data', help='S3 prefix/folder')
    parser.add_argument('--data-dir', default='neptune_export', help='Directory containing CSV files')
    parser.add_argument('--role-arn', help='IAM role ARN for Neptune Analytics to access S3')
    return parser.parse_args()

def upload_file_to_s3(file_path, s3_bucket, s3_key, region):
    """Upload file to S3"""
    s3 = boto3.client('s3', region_name=region)
    print(f"Uploading {file_path} to s3://{s3_bucket}/{s3_key}")
    s3.upload_file(file_path, s3_bucket, s3_key)
    return f"s3://{s3_bucket}/{s3_key}"

def load_data_to_neptune_analytics(endpoint, s3_uri, region, role_arn=None):
    """Load data from S3 to Neptune Analytics"""

    try:

        neptune_client = NeptuneAnalyticsClient(endpoint=endpoint, port=443)
        
        query = f"""CALL neptune.load({{format: "csv", 
                    source: "{s3_uri}", 
                    region: "us-west-2"}})"""

        response = neptune_client.execute_open_cypher(query)
        
        return response
    except Exception as e:
        logger.error(f"Error loading data into Neptune: {e}")
        return False

def main():
    args = parse_args()
    
    # Use existing S3 bucket from CDK stack
    print(f"Using S3 bucket: {args.s3_bucket}")
    
    # Get all CSV files in the data directory
    csv_files = [f for f in os.listdir(args.data_dir) if f.endswith('.csv')]
    
    if not csv_files:
        print(f"No CSV files found in {args.data_dir}")
        return
    
    # Upload each CSV file to S3
    s3_uris = []
    for csv_file in csv_files:
        file_path = os.path.join(args.data_dir, csv_file)
        s3_key = f"{args.s3_prefix}/{csv_file}"
        s3_uri = upload_file_to_s3(file_path, args.s3_bucket, s3_key, args.region)
        s3_uris.append(s3_uri)
    
    # Create S3 prefix URI for the folder containing all CSV files
    s3_prefix_uri = f"s3://{args.s3_bucket}/{args.s3_prefix}/"
    print(f"All files uploaded to {s3_prefix_uri}")
    
    # Load data to Neptune Analytics using the prefix URI
    status = load_data_to_neptune_analytics(args.endpoint, s3_prefix_uri, args.region, args.role_arn)
    print(status)
    if status:
        print("Data import completed successfully!")
    else:
        print(f"Import failed with status: {status}")
        print("Error details:")

if __name__ == "__main__":
    main()
