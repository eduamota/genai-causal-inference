# Causal Analysis with Neptune Analytics

This project demonstrates causal analysis using AWS Neptune Analytics.

## Project Structure

- `neptune_export/`: Contains CSV files with graph data (vertices and edges)
- `neptune_analytics_cdk/`: CDK project to deploy Neptune Analytics
- `upload_to_neptune_analytics.py`: Script to upload data to Neptune Analytics

## Setup and Deployment

### 1. Deploy Neptune Analytics using CDK

```bash
cd neptune_analytics_cdk
source .venv/bin/activate
pip install -r requirements.txt
cdk deploy
```

After deployment, note the following outputs from CloudFormation:
- Neptune Analytics endpoint (graph ID)
- S3 bucket name
- IAM role ARN for Neptune Analytics to access S3

### 2. Upload Data to Neptune Analytics

Use the provided script to upload data from the `neptune_export` directory:

```bash
# Install required packages
pip install boto3

# Run the upload script
python upload_to_neptune_analytics.py \
  --endpoint YOUR_NEPTUNE_ANALYTICS_ENDPOINT \
  --region YOUR_AWS_REGION \
  --s3-bucket YOUR_S3_BUCKET_NAME \
  --role-arn YOUR_IAM_ROLE_ARN
```

Replace:
- `YOUR_NEPTUNE_ANALYTICS_ENDPOINT` with the endpoint from CDK deployment
- `YOUR_AWS_REGION` with your AWS region (e.g., us-east-1)
- `YOUR_S3_BUCKET_NAME` with the S3 bucket name from CDK outputs
- `YOUR_IAM_ROLE_ARN` with the IAM role ARN from CDK outputs

### 3. Query the Graph

After data is loaded, you can query the graph using the Neptune Analytics query endpoint.

## Sample Queries

### 1. Count vertices by label
```cypher
// Count all vertices by label
MATCH (n)
RETURN labels(n) AS label, count(*) AS count
ORDER BY count DESC
```

### 2. Explore rating vertices
```cypher
// Get all rating vertices with their properties
MATCH (r:rating)
RETURN r
LIMIT 10
```

### 3. Explore resolution vertices
```cypher
// Get all resolution vertices
MATCH (r:resolution)
RETURN r
```

### 4. Explore understanding vertices
```cypher
// Get all understanding vertices
MATCH (u:understanding)
RETURN u
```

### 5. Explore ticket vertices with all properties
```cypher
// Get ticket vertices with all their properties
MATCH (t:ticket)
RETURN t
LIMIT 10
```

## Relationship Analysis Queries

### 6. Find relationships between resolution and ratings
```cypher
// Find how resolution affects ratings
MATCH (res:resolution)-[r:AFFECTS]->(rating:rating)
RETURN res.resolution_status, r.weight, rating.rating_numeric
LIMIT 20
```

### 7. Find relationships between understanding and ratings
```cypher
// Find how understanding influences ratings
MATCH (u:understanding)-[r:INFLUENCES]->(rating:rating)
RETURN u.understanding_status, rating.rating_numeric
LIMIT 20
```

### 8. Analyze tickets with their ratings
```cypher
// Find tickets with their ratings
MATCH (t:ticket)-[r:HAS_RATING]->(rating:rating)
RETURN t.custom_platform, t.resolution_status, t.understanding_status, rating.rating_numeric
LIMIT 20
```

## Advanced Analysis Queries

### 9. Analyze the impact of resolution on ratings
```cypher
// Calculate average rating by resolution status
MATCH (res:resolution)-[r:AFFECTS]->(rating:rating)
RETURN res.resolution_status, 
       avg(rating.rating_numeric) AS avg_rating,
       count(*) AS count
```

### 10. Analyze the impact of understanding on ratings
```cypher
// Calculate average rating by understanding status
MATCH (u:understanding)-[r:INFLUENCES]->(rating:rating)
RETURN u.understanding_status, 
       avg(rating.rating_numeric) AS avg_rating,
       count(*) AS count
```

### 11. Find the combined effect of resolution and understanding
```cypher
// Analyze how resolution and understanding together affect ratings
MATCH (res:resolution)-[:AFFECTS]->(rating:rating),
      (u:understanding)-[:INFLUENCES]->(rating)
RETURN res.resolution_status, 
       u.understanding_status,
       avg(rating.rating_numeric) AS avg_rating,
       count(*) AS count
ORDER BY avg_rating DESC
```

### 12. Analyze AWS vs non-AWS platforms
```cypher
// Compare ratings between AWS and non-AWS platforms
MATCH (t:ticket)-[:HAS_RATING]->(rating:rating)
RETURN t.aws_platform_numeric, 
       avg(rating.rating_numeric) AS avg_rating,
       count(*) AS count
```

### 13. Find tickets with positive sentiment but negative ratings
```cypher
// Find anomalies: positive sentiment but low rating
MATCH (t:ticket)-[:HAS_RATING]->(rating:rating)
WHERE t.sentiment_numeric = 1 AND rating.rating_numeric = 0
RETURN t.custom_platform, t.resolution_status, t.understanding_status
```

### 14. Find tickets with negative sentiment but positive ratings
````cypher
// Find anomalies: negative sentiment but high rating
MATCH (t:ticket)-[:HAS_RATING]->(rating:rating)
WHERE t.sentiment_numeric = 0 AND rating.rating_numeric = 1
RETURN t.custom_platform, t.resolution_status, t.understanding_status
````

### 15. Analyze the effect of support case usage
```cypher
// Analyze how support case usage affects ratings
MATCH (t:ticket)-[:HAS_RATING]->(rating:rating)
RETURN t.support_case_numeric, 
       avg(rating.rating_numeric) AS avg_rating,
       count(*) AS count
```

### 16. Find the strongest causal relationships
```cypher
// Find the strongest causal relationships by weight
MATCH (res:resolution)-[r:AFFECTS]->(rating:rating)
RETURN res.resolution_status, r.weight, rating.rating_numeric
ORDER BY abs(r.weight) DESC
LIMIT 10
```

### 17. Path analysis through the graph
```cypher
// Find paths from understanding through ratings to resolution
MATCH path = (u:understanding)-[:INFLUENCES]->(rating:rating)<-[:AFFECTS]-(res:resolution)
RETURN path
LIMIT 5
```

### 18. Product-specific analysis
```cypher
// Analyze ratings by product
MATCH (t:ticket)-[:HAS_RATING]->(rating:rating)
WHERE t.custom_product <> 'unknown'
RETURN t.custom_product, 
       avg(rating.rating_numeric) AS avg_rating,
       count(*) AS count
ORDER BY count DESC
```

### 19. Analyze resolution effect distribution
```cypher
// Analyze the distribution of resolution effects
MATCH (t:ticket)
RETURN 
  CASE 
    WHEN t.resolution_effect > 0.5 THEN 'Strong Positive'
    WHEN t.resolution_effect > 0 THEN 'Weak Positive'
    WHEN t.resolution_effect = 0 THEN 'Neutral'
    WHEN t.resolution_effect > -0.5 THEN 'Weak Negative'
    ELSE 'Strong Negative'
  END AS effect_category,
  count(*) AS count
```

### 20. Find tickets with the most extreme resolution effects
```cypher
// Find tickets with extreme resolution effects
MATCH (t:ticket)
RETURN t.custom_platform, t.resolution_status, t.understanding_status, 
       t.resolution_effect, t.rating_numeric
ORDER BY abs(t.resolution_effect) DESC
LIMIT 10
```
## Data Structure

The graph consists of:
- Vertices: rating, understanding, resolution
- Edges: connections between these entities

## Cleanup

To delete all resources:

```bash
cd neptune_analytics_cdk
cdk destroy
```
