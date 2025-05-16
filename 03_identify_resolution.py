#!/usr/bin/env python3
"""
Script to analyze comment history and determine if the issue was resolved
using Amazon Bedrock's Nova Lite model.
"""

import pandas as pd
import os
import sys
import time
import json
from tqdm import tqdm
import boto3

def load_data(file_path='numeric_results.pkl'):
    """
    Load the processed data from pickle file.
    
    Parameters:
    -----------
    file_path : str, default='numeric_results.pkl'
        Path to the pickle file with processed data
        
    Returns:
    --------
    pandas.DataFrame
        DataFrame containing the processed data
    """
    if not os.path.exists(file_path):
        print(f"Error: Data file '{file_path}' not found.")
        print("Please run 02_convert_to_numeric.py first to generate the processed data.")
        sys.exit(1)
    
    try:
        print(f"Loading data from {file_path}...")
        df = pd.read_pickle(file_path)
        print(f"Successfully loaded data with shape: {df.shape}")
        return df
    except Exception as e:
        print(f"Error loading data: {e}")
        sys.exit(1)

def analyze_resolution_with_nova(text, bedrock_client):
    """
    Use Amazon Bedrock's Nova Lite model to determine if an issue was resolved.
    
    Parameters:
    -----------
    text : str
        Comment history text to analyze
    bedrock_client : boto3.client
        Initialized Bedrock client
        
    Returns:
    --------
    str
        'resolved' or 'unresolved'
    """
    prompt = f"""Context: You are analyzing a support ticket conversation between a cloud architect and a customer.
Based on the conversation history, determine if the customer's issue was resolved or not.
Only respond with the word 'resolved' or 'unresolved'.

Conversation history:
{text}

Was the issue resolved?"""
    
    messages = [
        {"role": "user", "content": [{"text": prompt}]},
    ]

    inf_params = {"maxTokens": 100, "topP": 0.1, "temperature": 0.1}

    try:
        response = bedrock_client.converse(
            modelId="us.amazon.nova-lite-v1:0",
            messages=messages,
            inferenceConfig=inf_params
        )

        result = response["output"]["message"]["content"][0]["text"].lower()
        
        # Ensure we only return 'resolved' or 'unresolved'
        if 'resolved' in result and 'unresolved' not in result:
            return 'resolved'
        else:
            return 'unresolved'
            
    except Exception as e:
        print(f"Error analyzing resolution: {e}")
        return None

def process_resolution_batch(df, comment_column='comment_history_table_string', 
                            batch_size=10, max_retries=3, output_pickle='resolution_results.pkl'):
    """
    Process resolution analysis on the DataFrame in batches.
    
    Parameters:
    -----------
    df : pandas.DataFrame
        DataFrame containing the data
    comment_column : str
        Column name containing text for resolution analysis
    batch_size : int
        Number of comments to process in each batch
    max_retries : int
        Maximum number of retries for failed API calls
    output_pickle : str
        Path to save the processed DataFrame
        
    Returns:
    --------
    pandas.DataFrame
        DataFrame with resolution analysis results
    """
    if comment_column not in df.columns:
        print(f"Error: Column '{comment_column}' not found in DataFrame")
        return df
    
    # Initialize Bedrock client
    print("Initializing Amazon Bedrock client...")
    try:
        bedrock_client = boto3.client(service_name='bedrock-runtime')
    except Exception as e:
        print(f"Error initializing Bedrock client: {e}")
        print("Make sure you have the AWS CLI configured with appropriate credentials.")
        sys.exit(1)
    
    # Create a copy of the DataFrame
    result_df = df.copy()
    
    # Add resolution column if it doesn't exist
    if 'resolution_status' not in result_df.columns:
        result_df['resolution_status'] = None
    
    # Get comments that need processing
    comments_to_process = result_df[result_df['resolution_status'].isna()].index
    total_to_process = len(comments_to_process)
    
    if total_to_process == 0:
        print("All comments already have resolution analysis. Nothing to process.")
        return result_df
    
    print(f"Processing resolution analysis for {total_to_process} comments in batches of {batch_size}...")
    
    # Process in batches with progress bar
    for i in tqdm(range(0, total_to_process, batch_size)):
        batch_indices = comments_to_process[i:i+batch_size]
        
        for idx in batch_indices:
            comment = df.loc[idx, comment_column]
            
            # Skip empty comments
            if pd.isna(comment) or comment == "":
                result_df.loc[idx, 'resolution_status'] = 'unknown'
                continue
            
            # Try processing with retries
            retry_count = 0
            resolution = None
            
            while resolution is None and retry_count < max_retries:
                try:
                    resolution = analyze_resolution_with_nova(comment, bedrock_client)
                except Exception as e:
                    retry_count += 1
                    print(f"\nError processing comment {idx}: {e}")
                    if retry_count < max_retries:
                        print(f"Retrying (attempt {retry_count+1}/{max_retries})...")
                        time.sleep(2)  # Wait before retrying
                    else:
                        print(f"Failed after {max_retries} attempts. Skipping.")
            
            # Store result
            if resolution:
                result_df.loc[idx, 'resolution_status'] = resolution
            else:
                result_df.loc[idx, 'resolution_status'] = 'unknown'
        
        # Save intermediate results every 5 batches
        if (i // batch_size) % 5 == 0 and i > 0:
            try:
                print(f"\nSaving intermediate results...")
                result_df.to_pickle(output_pickle)
            except Exception as e:
                print(f"Warning: Failed to save intermediate results: {e}")

    # Convert resolution status to numeric
    result_df['resolution_numeric'] = result_df['resolution_status'].map({
        'resolved': 1,
        'unresolved': 0,
        'unknown': None
    })
    
    # Save final results
    try:
        print(f"Saving processed DataFrame to pickle file: {output_pickle}")
        result_df.to_pickle(output_pickle)
        print("Successfully saved processed DataFrame to pickle file")
    except Exception as e:
        print(f"Error saving processed pickle file: {e}")
    
    return result_df

def display_statistics(df):
    """Display statistics about the resolution analysis"""
    print("\n=== Resolution Analysis Statistics ===")
    
    if 'resolution_status' in df.columns:
        print("\nResolution status distribution:")
        resolution_counts = df['resolution_status'].value_counts()
        print(resolution_counts)
        
        # Calculate percentage
        resolution_pct = df['resolution_status'].value_counts(normalize=True) * 100
        print("\nResolution status percentage:")
        for value, pct in resolution_pct.items():
            print(f"  {value}: {pct:.2f}%")
    
    # Correlations with other numeric columns
    if 'resolution_numeric' in df.columns:
        print("\nCorrelations with resolution status:")
        numeric_cols = ['sentiment_numeric', 'rating_numeric', 'support_case_numeric', 
                       'aws_platform_numeric', 'resolution_numeric']
        available_cols = [col for col in numeric_cols if col in df.columns and col != 'resolution_numeric']
        
        for col in available_cols:
            correlation = df['resolution_numeric'].corr(df[col])
            print(f"  resolution_numeric vs {col}: {correlation:.4f}")

if __name__ == "__main__":
    try:
        # Load data
        df = load_data()
        
        # Check if resolution results already exist
        resolution_pickle = 'resolution_results.pkl'
        if os.path.exists(resolution_pickle):
            try:
                resolution_df = pd.read_pickle(resolution_pickle)
                print(f"Loaded existing resolution results from {resolution_pickle}")
                df = resolution_df
            except Exception as e:
                print(f"Error loading existing resolution results: {e}")
        
        # Process resolution analysis
        df = process_resolution_batch(
            df, 
            comment_column='comment_history_table_string',
            batch_size=10,
            output_pickle=resolution_pickle
        )
        
        # Display statistics
        display_statistics(df)
        
        print("\nResolution analysis complete. The DataFrame now contains:")
        print("- 'resolution_status': 'resolved', 'unresolved', or 'unknown'")
        print("- 'resolution_numeric': 1 for resolved, 0 for unresolved")
        print(f"\nResults saved to '{resolution_pickle}'")
        
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
