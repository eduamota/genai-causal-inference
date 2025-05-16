#!/usr/bin/env python3
"""
Script to read the ticket_sample.csv file and perform sentiment analysis
using Amazon Bedrock's Nova Lite model to classify comments as positive or negative.
"""

import pandas as pd
import os
import sys
import time
import json
from tqdm import tqdm
import boto3

def load_ticket_data(file_path='ticket_sample.csv', output_pickle='raw_ticket_data.pkl'):
    """
    Load the ticket sample data into a pandas DataFrame.
    
    Parameters:
    -----------
    file_path : str, default='ticket_sample.csv'
        Path to the CSV file
    output_pickle : str, default='raw_ticket_data.pkl'
        Path to save the raw DataFrame
        
    Returns:
    --------
    pandas.DataFrame
        DataFrame containing the ticket data
    """
    # Check if pickle file exists for faster loading
    if os.path.exists(output_pickle):
        try:
            print(f"Loading data from pickle file: {output_pickle}")
            df = pd.read_pickle(output_pickle)
            print(f"Successfully loaded data from pickle file")
            print(f"DataFrame shape: {df.shape}")
            return df
        except Exception as e:
            print(f"Error loading pickle file: {e}")
            print("Falling back to CSV loading")
    
    # Check if CSV file exists
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"The file {file_path} does not exist")
    
    # Read the CSV file
    try:
        print(f"Loading {file_path}...")
        df = pd.read_csv(file_path)
        print(f"Successfully loaded {file_path}")
        print(f"DataFrame shape: {df.shape}")
        
        # Save raw data to pickle for future use
        try:
            print(f"Saving raw DataFrame to pickle file: {output_pickle}")
            df.to_pickle(output_pickle)
            print("Successfully saved raw DataFrame to pickle file")
        except Exception as e:
            print(f"Warning: Error saving raw pickle file: {e}")
        
        return df
    except Exception as e:
        print(f"Error loading {file_path}: {e}")
        raise

def analyze_sentiment_with_nova(text, bedrock_client):
    """
    Use Amazon Bedrock's Nova Lite model to analyze sentiment of text.
    
    Parameters:
    -----------
    text : str
        Text to analyze
    bedrock_client : boto3.client
        Initialized Bedrock client
        
    Returns:
    --------
    str
        'positive' or 'negative'
    """
    prompt = f"""Context: You are gonna be given an email conversation between a cloud architect and a customer. The customer is looking for help. Analyze the sentiment of the following conversation and classify it as either 'positive' or 'negative'. 
Only respond with the word 'positive' or 'negative'.

Text: {text}

Sentiment:"""
    
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

        result = response["output"]["message"]["content"][0]["text"]
        
        # Ensure we only return 'positive' or 'negative'
        if 'positive' in result:
            return 'positive'
        elif 'negative' in result:
            return 'negative'
        else:
            # Default to negative if unclear
            return 'negative'
            
    except Exception as e:
        print(f"Error analyzing sentiment: {e}")
        return None

def process_sentiment_batch(df, comment_column, batch_size=10, max_retries=3, 
                           output_pickle='sentiment_results.pkl'):
    """
    Process sentiment analysis on the DataFrame in batches.
    
    Parameters:
    -----------
    df : pandas.DataFrame
        DataFrame containing the data
    comment_column : str
        Column name containing text for sentiment analysis
    batch_size : int
        Number of comments to process in each batch
    max_retries : int
        Maximum number of retries for failed API calls
    output_pickle : str
        Path to save the processed DataFrame
        
    Returns:
    --------
    pandas.DataFrame
        DataFrame with sentiment analysis results
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
    
    # Add sentiment column if it doesn't exist
    if 'sentiment' not in result_df.columns:
        result_df['sentiment'] = None
    
    # Get comments that need processing
    comments_to_process = result_df[result_df['sentiment'].isna()].index
    total_to_process = len(comments_to_process)
    
    if total_to_process == 0:
        print("All comments already have sentiment analysis. Nothing to process.")
        return result_df
    
    print(f"Processing sentiment analysis for {total_to_process} comments in batches of {batch_size}...")
    
    # Process in batches with progress bar
    for i in tqdm(range(0, total_to_process, batch_size)):
        batch_indices = comments_to_process[i:i+batch_size]
        
        for idx in batch_indices:
            comment = df.loc[idx, comment_column]
            
            # Skip empty comments
            if pd.isna(comment) or comment == "":
                result_df.loc[idx, 'sentiment'] = 'neutral'
                continue
            
            # Try processing with retries
            retry_count = 0
            sentiment = None
            
            while sentiment is None and retry_count < max_retries:
                try:
                    sentiment = analyze_sentiment_with_nova(comment, bedrock_client)
                except Exception as e:
                    retry_count += 1
                    print(f"\nError processing comment {idx}: {e}")
                    if retry_count < max_retries:
                        print(f"Retrying (attempt {retry_count+1}/{max_retries})...")
                        time.sleep(2)  # Wait before retrying
                    else:
                        print(f"Failed after {max_retries} attempts. Skipping.")
            
            # Store result
            if sentiment:
                result_df.loc[idx, 'sentiment'] = sentiment
            else:
                result_df.loc[idx, 'sentiment'] = 'unknown'
        
        # Save intermediate results every 5 batches
        if (i // batch_size) % 5 == 0 and i > 0:
            try:
                print(f"\nSaving intermediate results...")
                result_df.to_pickle(output_pickle)
            except Exception as e:
                print(f"Warning: Failed to save intermediate results: {e}")
    
    # Save final results
    try:
        print(f"Saving processed DataFrame to pickle file: {output_pickle}")
        result_df.to_pickle(output_pickle)
        print("Successfully saved processed DataFrame to pickle file")
    except Exception as e:
        print(f"Error saving processed pickle file: {e}")
    
    return result_df

if __name__ == "__main__":
    try:
        # Load data
        df = load_ticket_data()
        
        # Check if sentiment results already exist
        sentiment_pickle = 'sentiment_results.pkl'
        if os.path.exists(sentiment_pickle):
            try:
                sentiment_df = pd.read_pickle(sentiment_pickle)
                print(f"Loaded existing sentiment results from {sentiment_pickle}")
                df = sentiment_df
            except Exception as e:
                print(f"Error loading existing sentiment results: {e}")
        
        # Process sentiment analysis
        df = process_sentiment_batch(
            df, 
            comment_column='comment_history_table_string',
            batch_size=10,
            output_pickle=sentiment_pickle
        )
        
        # Display results
        print("\nSentiment analysis results:")
        sentiment_counts = df['sentiment'].value_counts()
        print(sentiment_counts)
        
        print("\nSample positive comments:")
        positive_samples = df[df['sentiment'] == 'positive'].head(3)
        for idx, row in positive_samples.iterrows():
            print(f"\n--- Comment {idx} ---")
            print(row['comment_history_table_string'][:200] + "..." if len(row['comment_history_table_string']) > 200 else row['comment_history_table_string'])
        
        print("\nSample negative comments:")
        negative_samples = df[df['sentiment'] == 'negative'].head(3)
        for idx, row in negative_samples.iterrows():
            print(f"\n--- Comment {idx} ---")
            print(row['comment_history_table_string'][:200] + "..." if len(row['comment_history_table_string']) > 200 else row['comment_history_table_string'])
        
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
