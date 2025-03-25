import psycopg2
import pandas as pd

# Database connection
conn = psycopg2.connect(
    dbname="twitter_etl",  # Your database name
    user="postgres",       # Your database username
    password="admin",      # Your database password
    host="localhost",      # Your database host (usually localhost)
    port="5432"            # Default PostgreSQL port
)

# Create a cursor
cur = conn.cursor()

# Query to fetch tweet data from the tweets table
query = "SELECT * FROM tweets;"

# Execute the query
cur.execute(query)

# Fetch all the records
records = cur.fetchall()

# Define column names for the DataFrame based on the table structure
columns = ['tweet_id', 'username', 'text', 'created_at', 'favorite_count', 'retweet_count']

# Load data into a pandas DataFrame
df = pd.DataFrame(records, columns=columns)

# Close the cursor and connection
cur.close()
conn.close()

# Display the first few rows of the DataFrame
print(df.head())

# Convert 'created_at' column to datetime
df['created_at'] = pd.to_datetime(df['created_at'])

# Extract Date, Hour, or Day from 'created_at' for analysis
df['date'] = df['created_at'].dt.date
df['hour'] = df['created_at'].dt.hour

# Check for missing values
print(df.isnull().sum())

import matplotlib.pyplot as plt

# Sort tweets by the number of likes (favorite_count)
top_liked_tweets = df.nlargest(10, 'favorite_count')

# Plot the top 10 most liked tweets
plt.figure(figsize=(10, 6))
plt.barh(top_liked_tweets['text'].head(10), top_liked_tweets['favorite_count'].head(10), color='orange')
plt.title("Top 10 Most Liked Tweets")
plt.xlabel("Number of Likes")
plt.ylabel("Tweet Text")
plt.gca().invert_yaxis()
plt.show()



