#!/usr/bin/env python3
"""
Script to evaluate if agents understood the customer's issue
using Amazon Bedrock's Nova Lite model.
"""

import pandas as pd
import os
import sys
import time
import json
from tqdm import tqdm
import boto3

def load_data(file_path='resolution_results.pkl'):
    """
    Load the processed data from pickle file.
    
    Parameters:
    -----------
    file_path : str, default='resolution_results.pkl'
        Path to the pickle file with processed data
        
    Returns:
    --------
    pandas.DataFrame
        DataFrame containing the processed data
    """
    if not os.path.exists(file_path):
        print(f"Error: Data file '{file_path}' not found.")
        print("Please run 03_identify_resolution.py first to generate the processed data.")
        sys.exit(1)
    
    try:
        print(f"Loading data from {file_path}...")
        df = pd.read_pickle(file_path)
        print(f"Successfully loaded data with shape: {df.shape}")
        return df
    except Exception as e:
        print(f"Error loading data: {e}")
        sys.exit(1)

def evaluate_understanding_with_nova(text, bedrock_client):
    """
    Use Amazon Bedrock's Nova Lite model to evaluate if the agent understood the customer's issue.
    
    Parameters:
    -----------
    text : str
        Comment history text to analyze
    bedrock_client : boto3.client
        Initialized Bedrock client
        
    Returns:
    --------
    str
        'understood' or 'misunderstood'
    """
    prompt = f"""Context: You are analyzing a support ticket conversation between a cloud architect and a customer.
Based on the conversation history, determine if the cloud architect correctly understood the customer's issue.
Look for evidence such as:
- Did the architect correctly identify the root problem?
- Did the architect ask relevant clarifying questions?
- Did the architect's proposed solutions address the actual issue?
- Did the customer need to repeatedly explain the same issue?

Only respond with the word 'understood' or 'misunderstood'.

Conversation history:
{text}

Did the cloud architect understand the customer's issue?"""
    
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
        
        # Ensure we only return 'understood' or 'misunderstood'
        if 'understood' in result and 'misunderstood' not in result:
            return 'understood'
        else:
            return 'misunderstood'
            
    except Exception as e:
        print(f"Error evaluating understanding: {e}")
        return None

def process_understanding_batch(df, comment_column='comment_history_table_string', 
                               batch_size=10, max_retries=3, output_pickle='understanding_results.pkl'):
    """
    Process understanding evaluation on the DataFrame in batches.
    
    Parameters:
    -----------
    df : pandas.DataFrame
        DataFrame containing the data
    comment_column : str
        Column name containing text for understanding evaluation
    batch_size : int
        Number of comments to process in each batch
    max_retries : int
        Maximum number of retries for failed API calls
    output_pickle : str
        Path to save the processed DataFrame
        
    Returns:
    --------
    pandas.DataFrame
        DataFrame with understanding evaluation results
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
    
    # Add understanding column if it doesn't exist
    if 'understanding_status' not in result_df.columns:
        result_df['understanding_status'] = None
    
    # Get comments that need processing
    comments_to_process = result_df[result_df['understanding_status'].isna()].index
    total_to_process = len(comments_to_process)
    
    if total_to_process == 0:
        print("All comments already have understanding evaluation. Nothing to process.")
        return result_df
    
    print(f"Processing understanding evaluation for {total_to_process} comments in batches of {batch_size}...")
    
    # Process in batches with progress bar
    for i in tqdm(range(0, total_to_process, batch_size)):
        batch_indices = comments_to_process[i:i+batch_size]
        
        for idx in batch_indices:
            comment = df.loc[idx, comment_column]
            
            # Skip empty comments
            if pd.isna(comment) or comment == "":
                result_df.loc[idx, 'understanding_status'] = 'unknown'
                continue
            
            # Try processing with retries
            retry_count = 0
            understanding = None
            
            while understanding is None and retry_count < max_retries:
                try:
                    understanding = evaluate_understanding_with_nova(comment, bedrock_client)
                except Exception as e:
                    retry_count += 1
                    print(f"\nError processing comment {idx}: {e}")
                    if retry_count < max_retries:
                        print(f"Retrying (attempt {retry_count+1}/{max_retries})...")
                        time.sleep(2)  # Wait before retrying
                    else:
                        print(f"Failed after {max_retries} attempts. Skipping.")
            
            # Store result
            if understanding:
                result_df.loc[idx, 'understanding_status'] = understanding
            else:
                result_df.loc[idx, 'understanding_status'] = 'unknown'
        
        # Save intermediate results every 5 batches
        if (i // batch_size) % 5 == 0 and i > 0:
            try:
                print(f"\nSaving intermediate results...")
                result_df.to_pickle(output_pickle)
            except Exception as e:
                print(f"Warning: Failed to save intermediate results: {e}")

    # Convert understanding status to numeric
    result_df['understanding_numeric'] = result_df['understanding_status'].map({
        'understood': 1,
        'misunderstood': 0,
        'unknown': None
    })
    
    # Fill any remaining NaN values with 0 (treat unknown as misunderstood)
    result_df['understanding_numeric'] = result_df['understanding_numeric'].fillna(0)
    
    # Save final results
    try:
        print(f"Saving processed DataFrame to pickle file: {output_pickle}")
        result_df.to_pickle(output_pickle)
        print("Successfully saved processed DataFrame to pickle file")
    except Exception as e:
        print(f"Error saving processed pickle file: {e}")
    
    
    
    return result_df

def display_statistics(df):
    """Display statistics about the understanding evaluation"""
    print("\n=== Understanding Evaluation Statistics ===")
    
    if 'understanding_status' in df.columns:
        print("\nUnderstanding status distribution:")
        understanding_counts = df['understanding_status'].value_counts()
        print(understanding_counts)
        
        # Calculate percentage
        understanding_pct = df['understanding_status'].value_counts(normalize=True) * 100
        print("\nUnderstanding status percentage:")
        for value, pct in understanding_pct.items():
            print(f"  {value}: {pct:.2f}%")
    
    # Correlations with other numeric columns
    if 'understanding_numeric' in df.columns:
        print("\nCorrelations with understanding status:")
        numeric_cols = ['sentiment_numeric', 'rating_numeric', 'support_case_numeric', 
                       'aws_platform_numeric', 'resolution_numeric', 'understanding_numeric']
        available_cols = [col for col in numeric_cols if col in df.columns and col != 'understanding_numeric']
        
        for col in available_cols:
            correlation = df['understanding_numeric'].corr(df[col])
            print(f"  understanding_numeric vs {col}: {correlation:.4f}")
        
        # Analyze relationship between understanding and resolution
        if 'resolution_numeric' in df.columns:
            print("\nRelationship between understanding and resolution:")
            # Create a cross-tabulation
            cross_tab = pd.crosstab(
                df['understanding_numeric'], 
                df['resolution_numeric'],
                normalize='index'
            ) * 100
            
            print("Percentage of issues resolved when:")
            if 1.0 in cross_tab.columns:
                if 1.0 in cross_tab.index:
                    print(f"  Agent understood the issue: {cross_tab.loc[1.0, 1.0]:.2f}%")
                if 0.0 in cross_tab.index:
                    print(f"  Agent misunderstood the issue: {cross_tab.loc[0.0, 1.0]:.2f}%")

if __name__ == "__main__":
    try:
        # Load data
        df = load_data()
        
        # Check if understanding results already exist
        understanding_pickle = 'understanding_results.pkl'
        if os.path.exists(understanding_pickle):
            try:
                understanding_df = pd.read_pickle(understanding_pickle)
                print(f"Loaded existing understanding results from {understanding_pickle}")
                df = understanding_df
            except Exception as e:
                print(f"Error loading existing understanding results: {e}")
        
        # Process understanding evaluation
        df = process_understanding_batch(
            df, 
            comment_column='comment_history_table_string',
            batch_size=10,
            output_pickle=understanding_pickle
        )
        
        # Display statistics
        display_statistics(df)
        
        print("\nUnderstanding evaluation complete. The DataFrame now contains:")
        print("- 'understanding_status': 'understood', 'misunderstood', or 'unknown'")
        print("- 'understanding_numeric': 1 for understood, 0 for misunderstood/unknown")
        print(f"\nResults saved to '{understanding_pickle}'")
        
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
