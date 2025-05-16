import pandas as pd
import os
import pickle

def convert_to_neptune_gremlin_csv(df, output_dir):
    """
    Convert a dataframe to Neptune-compatible Gremlin CSV format
    """
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # 1. Create vertices files
    
    # Vertices for rating entities
    rating_vertices = df.copy()
    rating_vertices['~id'] = 'rating_' + rating_vertices.index.astype(str)
    rating_vertices['~label'] = 'rating'
    
    # Handle boolean values by converting to int
    if 'cloud_support_case_used' in rating_vertices.columns:
        rating_vertices['cloud_support_case_used'] = rating_vertices['cloud_support_case_used'].astype(int)
    
    # Handle NaN values in custom_product
    if 'custom_product' in rating_vertices.columns:
        rating_vertices['custom_product'] = rating_vertices['custom_product'].fillna("unknown")
    
    # Move the ~id and ~label columns to the front
    cols = ['~id', '~label'] + [col for col in rating_vertices.columns if col not in ['~id', '~label']]
    rating_vertices = rating_vertices[cols]
    
    # Write rating vertices to CSV
    rating_vertices.to_csv(f"{output_dir}/rating_vertices.csv", index=False)
    
    # Create resolution vertices
    resolution_vertices = df[['resolution_status', 'resolution_numeric']].drop_duplicates()
    resolution_vertices['~id'] = 'resolution_' + resolution_vertices['resolution_status']
    resolution_vertices['~label'] = 'resolution'
    
    # Move the ~id and ~label columns to the front
    cols = ['~id', '~label'] + [col for col in resolution_vertices.columns if col not in ['~id', '~label']]
    resolution_vertices = resolution_vertices[cols]
    
    resolution_vertices.to_csv(f"{output_dir}/resolution_vertices.csv", index=False)
    
    # Create understanding vertices
    understanding_vertices = df[['understanding_status', 'understanding_numeric']].drop_duplicates()
    understanding_vertices['~id'] = 'understanding_' + understanding_vertices['understanding_status']
    understanding_vertices['~label'] = 'understanding'
    
    # Move the ~id and ~label columns to the front
    cols = ['~id', '~label'] + [col for col in understanding_vertices.columns if col not in ['~id', '~label']]
    understanding_vertices = understanding_vertices[cols]
    
    understanding_vertices.to_csv(f"{output_dir}/understanding_vertices.csv", index=False)
    
    # Create ticket vertices (to store all original data)
    ticket_vertices = df.copy()
    
    # Handle boolean values by converting to int
    if 'cloud_support_case_used' in ticket_vertices.columns:
        ticket_vertices['cloud_support_case_used'] = ticket_vertices['cloud_support_case_used'].astype(int)
    
    # Handle NaN values in custom_product
    if 'custom_product' in ticket_vertices.columns:
        ticket_vertices['custom_product'] = ticket_vertices['custom_product'].fillna("unknown")
    
    ticket_vertices['~id'] = 'ticket_' + ticket_vertices.index.astype(str)
    ticket_vertices['~label'] = 'ticket'
    
    # Move the ~id and ~label columns to the front
    cols = ['~id', '~label'] + [col for col in ticket_vertices.columns if col not in ['~id', '~label']]
    ticket_vertices = ticket_vertices[cols]
    
    ticket_vertices.to_csv(f"{output_dir}/ticket_vertices.csv", index=False)
    
    # 2. Create edges files
    
    # Resolution to Rating edges with resolution_effect as weight
    resolution_rating_edges = df.copy()
    
    # Handle boolean values by converting to int
    if 'cloud_support_case_used' in resolution_rating_edges.columns:
        resolution_rating_edges['cloud_support_case_used'] = resolution_rating_edges['cloud_support_case_used'].astype(int)
    
    resolution_rating_edges['~id'] = 'edge_res_rating_' + resolution_rating_edges.index.astype(str)
    resolution_rating_edges['~from'] = 'resolution_' + resolution_rating_edges['resolution_status']
    resolution_rating_edges['~to'] = 'rating_' + resolution_rating_edges.index.astype(str)
    resolution_rating_edges['~label'] = 'AFFECTS'
    resolution_rating_edges['weight'] = resolution_rating_edges['resolution_effect']
    
    # Select relevant columns for edges
    edge_columns = ['~id', '~from', '~to', '~label', 'weight']
    resolution_rating_edges = resolution_rating_edges[edge_columns]
    resolution_rating_edges.to_csv(f"{output_dir}/resolution_rating_edges.csv", index=False)
    
    # Understanding to Rating edges
    understanding_rating_edges = df.copy()
    understanding_rating_edges['~id'] = 'edge_und_rating_' + understanding_rating_edges.index.astype(str)
    understanding_rating_edges['~from'] = 'understanding_' + understanding_rating_edges['understanding_status']
    understanding_rating_edges['~to'] = 'rating_' + understanding_rating_edges.index.astype(str)
    understanding_rating_edges['~label'] = 'INFLUENCES'
    
    # Include only the required edge columns
    edge_columns = ['~id', '~from', '~to', '~label']
    understanding_rating_edges = understanding_rating_edges[edge_columns]
    understanding_rating_edges.to_csv(f"{output_dir}/understanding_rating_edges.csv", index=False)
    
    # Ticket to Rating edges
    ticket_rating_edges = df.copy()
    ticket_rating_edges['~id'] = 'edge_ticket_rating_' + ticket_rating_edges.index.astype(str)
    ticket_rating_edges['~from'] = 'ticket_' + ticket_rating_edges.index.astype(str)
    ticket_rating_edges['~to'] = 'rating_' + ticket_rating_edges.index.astype(str)
    ticket_rating_edges['~label'] = 'HAS_RATING'
    
    # Include only the required edge columns
    edge_columns = ['~id', '~from', '~to', '~label']
    ticket_rating_edges = ticket_rating_edges[edge_columns]
    ticket_rating_edges.to_csv(f"{output_dir}/ticket_rating_edges.csv", index=False)
    
    print(f"Gremlin CSV files have been created in the {output_dir} directory")
    print("Files created:")
    print(f"  - {output_dir}/rating_vertices.csv")
    print(f"  - {output_dir}/resolution_vertices.csv")
    print(f"  - {output_dir}/understanding_vertices.csv")
    print(f"  - {output_dir}/ticket_vertices.csv")
    print(f"  - {output_dir}/resolution_rating_edges.csv")
    print(f"  - {output_dir}/understanding_rating_edges.csv")
    print(f"  - {output_dir}/ticket_rating_edges.csv")
    
    return {
        "rating_vertices": rating_vertices,
        "resolution_vertices": resolution_vertices,
        "understanding_vertices": understanding_vertices,
        "ticket_vertices": ticket_vertices,
        "resolution_rating_edges": resolution_rating_edges,
        "understanding_rating_edges": understanding_rating_edges,
        "ticket_rating_edges": ticket_rating_edges
    }

# Example usage:
# Assuming your dataframe is called df

import pickle

with open('causal_analysis.pkl', 'rb') as f:
    df = pickle.load(f)

convert_to_neptune_gremlin_csv(df, 'neptune_export')