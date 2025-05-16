#!/usr/bin/env python3
"""
Script to load the causal analysis DataFrame and convert it to Neptune bulk load format
for loading into Amazon Neptune using the bulk loader.
"""

import pandas as pd
import os
import sys
import json
import uuid
import csv

def load_causal_data(file_path='causal_analysis.pkl'):
    """
    Load the causal analysis data from pickle file.
    
    Parameters:
    -----------
    file_path : str, default='causal_analysis.pkl'
        Path to the pickle file with causal analysis results
        
    Returns:
    --------
    pandas.DataFrame
        DataFrame containing the causal analysis data
    """
    if not os.path.exists(file_path):
        print(f"Error: Causal analysis file '{file_path}' not found.")
        sys.exit(1)
    
    try:
        print(f"Loading data from {file_path}...")
        df = pd.read_pickle(file_path)
        print(f"Successfully loaded data with shape: {df.shape}")
        return df
    except Exception as e:
        print(f"Error loading data: {e}")
        sys.exit(1)

def create_neptune_bulk_vertices(df):
    """
    Create Neptune bulk load format vertices from the DataFrame.
    
    Parameters:
    -----------
    df : pandas.DataFrame
        DataFrame containing the causal analysis data
        
    Returns:
    --------
    list
        List of vertex dictionaries in Neptune bulk load format
    """
    vertices = []
    
    # Create ticket vertices
    for idx, row in df.iterrows():
        ticket_id = str(row.get('ticket_id', f"ticket_{idx}"))
        
        # Create ticket vertex
        ticket_vertex = {
            "~id": f"ticket:{ticket_id}",
            "~label": "ticket",
            "ticket_id": ticket_id,
            "sentiment": int(row.get('sentiment_numeric', 0)),
            "rating": int(row.get('rating_numeric', 0)),
            "resolution": int(row.get('resolution_numeric', 0)),
            "understanding": int(row.get('understanding_numeric', 0)),
            "support_case": int(row.get('support_case_numeric', 0)),
            "aws_platform": int(row.get('aws_platform_numeric', 0))
        }
        
        # Add any additional properties that exist
        for col in df.columns:
            if col not in ['ticket_id', 'sentiment_numeric', 'rating_numeric', 
                          'resolution_numeric', 'understanding_numeric', 
                          'support_case_numeric', 'aws_platform_numeric',
                          'resolution_effect']:
                if not pd.isna(row.get(col)):
                    ticket_vertex[col] = row.get(col)
        
        vertices.append(ticket_vertex)
    
    # Create causal factor vertices
    resolution_vertex = {
        "~id": "factor:resolution",
        "~label": "factor",
        "name": "Resolution Status",
        "description": "Whether the customer issue was resolved or not"
    }
    
    rating_vertex = {
        "~id": "factor:rating",
        "~label": "factor",
        "name": "Customer Rating",
        "description": "Rating provided by the customer"
    }
    
    vertices.append(resolution_vertex)
    vertices.append(rating_vertex)
    
    return vertices

def create_neptune_bulk_edges(df):
    """
    Create Neptune bulk load format edges from the DataFrame.
    
    Parameters:
    -----------
    df : pandas.DataFrame
        DataFrame containing the causal analysis data
        
    Returns:
    --------
    list
        List of edge dictionaries in Neptune bulk load format
    """
    edges = []
    
    # Create edges between tickets and factors
    for idx, row in df.iterrows():
        ticket_id = str(row.get('ticket_id', f"ticket_{idx}"))
        
        # Create edges from ticket to factors
        resolution_edge = {
            "~id": f"e{uuid.uuid4()}",
            "~from": f"ticket:{ticket_id}",
            "~to": "factor:resolution",
            "~label": "has_factor",
            "value": int(row.get('resolution_numeric', 0))
        }
        
        rating_edge = {
            "~id": f"e{uuid.uuid4()}",
            "~from": f"ticket:{ticket_id}",
            "~to": "factor:rating",
            "~label": "has_factor",
            "value": int(row.get('rating_numeric', 0))
        }
        
        edges.append(resolution_edge)
        edges.append(rating_edge)
    
    # Create causal effect edge
    if 'resolution_effect' in df.columns:
        # Get the average causal effect if it exists
        causal_effect = df['resolution_effect'].mean() if not df['resolution_effect'].empty else 0
        
        causal_edge = {
            "~id": "e_causal_resolution_rating",
            "~from": "factor:resolution",
            "~to": "factor:rating",
            "~label": "causes",
            "effect_strength": float(causal_effect),
            "description": "Causal effect of resolution status on customer rating"
        }
        
        edges.append(causal_edge)
    
    return edges

def write_neptune_bulk_files(vertices, edges, vertices_file='neptune_vertices.csv', edges_file='neptune_edges.csv'):
    """
    Write vertices and edges to CSV files in Neptune bulk load format.
    
    Parameters:
    -----------
    vertices : list
        List of vertex dictionaries
    edges : list
        List of edge dictionaries
    vertices_file : str
        Output file for vertices
    edges_file : str
        Output file for edges
    """
    try:
        # Write vertices
        with open(vertices_file, 'w') as f:
            for vertex in vertices:
                f.write(f"{json.dumps(vertex)}\n")
        
        print(f"Successfully wrote {len(vertices)} vertices to {vertices_file}")
        
        # Write edges
        with open(edges_file, 'w') as f:
            for edge in edges:
                f.write(f"{json.dumps(edge)}\n")
        
        print(f"Successfully wrote {len(edges)} edges to {edges_file}")
        
    except Exception as e:
        print(f"Error writing Neptune bulk files: {e}")

def generate_neptune_bulk_load_command():
    """
    Generate Neptune bulk load command for loading data into Neptune.
    
    Returns:
    --------
    str
        Neptune bulk load command
    """
    bulk_load_command = """
# Neptune Bulk Load Command

# Replace these values with your actual Neptune and S3 details
NEPTUNE_ENDPOINT="your-neptune-endpoint.amazonaws.com"
S3_BUCKET="your-s3-bucket"
S3_FOLDER="causal-analysis"
IAM_ROLE_ARN="arn:aws:iam::your-account-id:role/NeptuneLoadFromS3"

# Upload files to S3
aws s3 cp neptune_vertices.csv s3://${S3_BUCKET}/${S3_FOLDER}/
aws s3 cp neptune_edges.csv s3://${S3_BUCKET}/${S3_FOLDER}/

# Start the bulk load job
curl -X POST \\
    -H "Content-Type: application/json" \\
    https://${NEPTUNE_ENDPOINT}:8182/loader -d \\
    '{
      "source": "s3://'${S3_BUCKET}'/'${S3_FOLDER}'/",
      "format": "csv",
      "iamRoleArn": "'${IAM_ROLE_ARN}'",
      "region": "us-east-1",
      "failOnError": "FALSE",
      "parallelism": "MEDIUM"
    }'

# Check the status of the load job
# Replace <load-id> with the ID returned from the previous command
# curl -G https://${NEPTUNE_ENDPOINT}:8182/loader/<load-id>
"""
    
    # Write command to file
    with open('neptune_bulk_load_command.sh', 'w') as f:
        f.write(bulk_load_command)
    
    print(f"Neptune bulk load command written to neptune_bulk_load_command.sh")
    
    return bulk_load_command

def generate_sample_queries():
    """
    Generate sample Gremlin queries for exploring the causal graph.
    
    Returns:
    --------
    str
        Sample Gremlin queries
    """
    sample_queries = """
// Sample Gremlin Queries for Causal Analysis Graph

// Connect to Neptune
:remote connect tinkerpop.server conf/remote.yaml
:remote console

// Count vertices and edges
g.V().count()
g.E().count()

// Get all factors
g.V().hasLabel('factor').valueMap(true)

// Get the causal relationship between resolution and rating
g.V().has('factor', 'name', 'Resolution Status')
  .outE('causes')
  .valueMap()

// Get tickets with positive resolution and their ratings
g.V().hasLabel('ticket').has('resolution', 1)
  .outE('has_factor').inV().has('factor', 'name', 'Customer Rating')
  .path().by(valueMap('resolution', 'rating'))

// Calculate average rating for resolved vs unresolved tickets
g.V().hasLabel('ticket').has('resolution', 1).values('rating').mean()
g.V().hasLabel('ticket').has('resolution', 0).values('rating').mean()

// Find tickets where agent understood the issue but it wasn't resolved
g.V().hasLabel('ticket')
  .has('understanding', 1)
  .has('resolution', 0)
  .valueMap('ticket_id', 'understanding', 'resolution', 'rating')

// Analyze the relationship between sentiment and resolution
g.V().hasLabel('ticket')
  .group()
    .by('sentiment')
    .by(values('resolution').mean())
"""
    
    # Write queries to file
    with open('neptune_sample_queries.txt', 'w') as f:
        f.write(sample_queries)
    
    print(f"Sample Gremlin queries written to neptune_sample_queries.txt")
    
    return sample_queries

if __name__ == "__main__":
    try:
        # Load causal analysis data
        df = load_causal_data()
        
        # Check if resolution_effect column exists
        if 'resolution_effect' not in df.columns:
            print("Warning: 'resolution_effect' column not found in the DataFrame.")
            print("This column is expected to contain the causal effect percentage.")
            
            # Create a dummy resolution_effect column for demonstration
            print("Creating a dummy resolution_effect column with value 0.5 for demonstration.")
            df['resolution_effect'] = 0.5
        
        # Create Neptune bulk load format vertices and edges
        vertices = create_neptune_bulk_vertices(df)
        edges = create_neptune_bulk_edges(df)
        
        # Write to files
        write_neptune_bulk_files(vertices, edges)
        
        # Generate bulk load command
        bulk_load_command = generate_neptune_bulk_load_command()
        
        # Generate sample queries
        sample_queries = generate_sample_queries()
        
        print("\nProcess complete. The following files have been created:")
        print("1. neptune_vertices.csv - Contains vertex data in Neptune bulk load format")
        print("2. neptune_edges.csv - Contains edge data in Neptune bulk load format")
        print("3. neptune_bulk_load_command.sh - Contains commands to load data into Neptune")
        print("4. neptune_sample_queries.txt - Contains sample Gremlin queries for exploring the data")
        
        print("\nTo load this data into Neptune:")
        print("1. Edit neptune_bulk_load_command.sh with your Neptune endpoint, S3 bucket, and IAM role")
        print("2. Upload the CSV files to S3")
        print("3. Execute the bulk load command")
        print("4. Use the sample queries to explore the causal graph")
        
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
