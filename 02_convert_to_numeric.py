#!/usr/bin/env python3
"""
Script to convert text and boolean values to numeric format:
- Convert sentiment: 'positive' -> 1, 'negative' -> 0
- Convert rating: 'good' -> 1, 'offered' -> 0
- Convert cloud_support_case_used: True -> 1, False -> 0
- Convert custom_platform: 'amazon_web_services' -> 1, everything else -> 0
"""

import pandas as pd
import os
import sys

def load_sentiment_data(file_path='sentiment_results.pkl'):
    """
    Load the sentiment analysis results from pickle file.
    
    Parameters:
    -----------
    file_path : str, default='sentiment_results.pkl'
        Path to the pickle file with sentiment results
        
    Returns:
    --------
    pandas.DataFrame
        DataFrame containing the data with sentiment analysis
    """
    if not os.path.exists(file_path):
        print(f"Error: Sentiment results file '{file_path}' not found.")
        print("Please run sentiment_analysis.py first to generate sentiment results.")
        sys.exit(1)
    
    try:
        print(f"Loading data from {file_path}...")
        df = pd.read_pickle(file_path)
        print(f"Successfully loaded data with shape: {df.shape}")
        return df
    except Exception as e:
        print(f"Error loading data: {e}")
        sys.exit(1)

def convert_to_numeric(df, output_pickle='numeric_results.pkl'):
    """
    Convert text categories to numeric values:
    - sentiment: 'positive' -> 1, 'negative' -> 0
    - rating: 'good' -> 1, 'offered' -> 0
    - cloud_support_case_used: True -> 1, False -> 0
    - custom_platform: 'amazon_web_services' -> 1, everything else -> 0
    
    Parameters:
    -----------
    df : pandas.DataFrame
        DataFrame containing the data
    output_pickle : str, default='numeric_results.pkl'
        Path to save the processed DataFrame
        
    Returns:
    --------
    pandas.DataFrame
        DataFrame with numeric values
    """
    # Create a copy of the DataFrame
    result_df = df.copy()
    
    # Convert sentiment to numeric
    if 'sentiment' in result_df.columns:
        print("Converting sentiment to numeric values...")
        # Create a new column for numeric sentiment
        result_df['sentiment_numeric'] = result_df['sentiment'].map({
            'positive': 1,
            'negative': 0,
            'neutral': None,
            'unknown': None
        })
        print(f"Converted sentiment values. Unique values: {result_df['sentiment_numeric'].unique()}")
    else:
        print("Warning: 'sentiment' column not found in DataFrame")
    
    # Convert rating to numeric
    if 'rating' in result_df.columns:
        print("Converting rating to numeric values...")
        # Check unique values in rating column
        unique_ratings = result_df['rating'].unique()
        print(f"Unique rating values found: {unique_ratings}")
        
        # Create a new column for numeric rating
        result_df['rating_numeric'] = result_df['rating'].map({
            'good': 1,
            'offered': 0
        })
        print(f"Converted rating values. Unique values: {result_df['rating_numeric'].unique()}")
    else:
        print("Warning: 'rating' column not found in DataFrame")
    
    # Convert cloud_support_case_used to numeric
    if 'cloud_support_case_used' in result_df.columns:
        print("Converting cloud_support_case_used to numeric values...")
        # Check unique values
        unique_support = result_df['cloud_support_case_used'].unique()
        print(f"Unique cloud_support_case_used values found: {unique_support}")
        
        # Create a new column for numeric cloud_support_case_used
        result_df['support_case_numeric'] = result_df['cloud_support_case_used'].astype(int)
        print(f"Converted cloud_support_case_used values. Unique values: {result_df['support_case_numeric'].unique()}")
    else:
        print("Warning: 'cloud_support_case_used' column not found in DataFrame")
    
    # Convert custom_platform to numeric (AWS vs non-AWS)
    if 'custom_platform' in result_df.columns:
        print("Converting custom_platform to numeric values...")
        # Check unique values
        unique_platforms = result_df['custom_platform'].unique()
        print(f"Unique custom_platform values found: {unique_platforms}")
        
        # Create a new column for numeric platform (AWS = 1, others = 0)
        result_df['aws_platform_numeric'] = result_df['custom_platform'].apply(
            lambda x: 1 if x == 'amazon_web_services' else 0
        )
        print(f"Converted custom_platform values. Unique values: {result_df['aws_platform_numeric'].unique()}")
    else:
        print("Warning: 'custom_platform' column not found in DataFrame")
    
    # Save the processed DataFrame
    try:
        print(f"Saving processed DataFrame to {output_pickle}...")
        result_df.to_pickle(output_pickle)
        print("Successfully saved processed DataFrame")
    except Exception as e:
        print(f"Error saving processed DataFrame: {e}")
    
    return result_df

def display_statistics(df):
    """Display statistics about the numeric columns"""
    print("\n=== Statistics ===")
    
    # Sentiment statistics
    if 'sentiment_numeric' in df.columns:
        print("\nSentiment distribution:")
        sentiment_counts = df['sentiment_numeric'].value_counts()
        print(sentiment_counts)
        
        # Calculate percentage
        sentiment_pct = df['sentiment_numeric'].value_counts(normalize=True) * 100
        print("\nSentiment percentage:")
        for value, pct in sentiment_pct.items():
            print(f"  {value}: {pct:.2f}%")
    
    # Rating statistics
    if 'rating_numeric' in df.columns:
        print("\nRating distribution:")
        rating_counts = df['rating_numeric'].value_counts()
        print(rating_counts)
        
        # Calculate percentage
        rating_pct = df['rating_numeric'].value_counts(normalize=True) * 100
        print("\nRating percentage:")
        for value, pct in rating_pct.items():
            print(f"  {value}: {pct:.2f}%")
    
    # Support case statistics
    if 'support_case_numeric' in df.columns:
        print("\nCloud support case usage distribution:")
        support_counts = df['support_case_numeric'].value_counts()
        print(support_counts)
        
        # Calculate percentage
        support_pct = df['support_case_numeric'].value_counts(normalize=True) * 100
        print("\nCloud support case usage percentage:")
        for value, pct in support_pct.items():
            print(f"  {value}: {pct:.2f}%")
    
    # AWS platform statistics
    if 'aws_platform_numeric' in df.columns:
        print("\nAWS platform distribution:")
        platform_counts = df['aws_platform_numeric'].value_counts()
        print(platform_counts)
        
        # Calculate percentage
        platform_pct = df['aws_platform_numeric'].value_counts(normalize=True) * 100
        print("\nAWS platform percentage:")
        for value, pct in platform_pct.items():
            print(f"  {value} ({'AWS' if value == 1 else 'Other'}): {pct:.2f}%")
    
    # Correlations
    print("\nCorrelations:")
    numeric_cols = ['sentiment_numeric', 'rating_numeric', 'support_case_numeric', 'aws_platform_numeric']
    available_cols = [col for col in numeric_cols if col in df.columns]
    
    if len(available_cols) >= 2:
        for i in range(len(available_cols)):
            for j in range(i+1, len(available_cols)):
                col1 = available_cols[i]
                col2 = available_cols[j]
                correlation = df[col1].corr(df[col2])
                print(f"  {col1} vs {col2}: {correlation:.4f}")

if __name__ == "__main__":
    try:
        # Load sentiment data
        df = load_sentiment_data()
        
        # Convert text categories to numeric values
        df = convert_to_numeric(df)
        
        # Display statistics
        display_statistics(df)
        
        print("\nConversion complete. The DataFrame now contains:")
        print("- 'sentiment_numeric': 1 for positive, 0 for negative")
        print("- 'rating_numeric': 1 for good, 0 for offered")
        print("- 'support_case_numeric': 1 for True, 0 for False")
        print("- 'aws_platform_numeric': 1 for amazon_web_services, 0 for other platforms")
        print(f"\nResults saved to 'numeric_results.pkl'")
        
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
