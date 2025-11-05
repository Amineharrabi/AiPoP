import os
import time
import pandas as pd
import praw
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from datetime import datetime
from dotenv import load_dotenv
from itertools import islice

# Load environment variables from .env
load_dotenv()

# Read Reddit credentials from environment variables (preferred)
def _clean_env(key, default=''):
    v = os.getenv(key, default)
    if v is None:
        return default
    v = v.strip()
    # treat commented values or placeholder comments as empty
    if v.startswith('#') or v == '':
        return ''
    return v

REDDIT_CLIENT_ID = _clean_env('REDDIT_CLIENT_ID')
REDDIT_CLIENT_SECRET = _clean_env('REDDIT_CLIENT_SECRET')
REDDIT_USERNAME = _clean_env('REDDIT_USERNAME')
REDDIT_PASSWORD = _clean_env('REDDIT_PASSWORD')
REDDIT_USER_AGENT = _clean_env('REDDIT_USER_AGENT') or 'ai-bubble-detector by u/yourusername'

# Enhanced subreddits for better AI sentiment tracking
SUBREDDITS = [
    'ArtificialInteligence', 'MachineLearning', 'technology', 'stocks',
    'singularity', 'deeplearning', 'neuralnetworks', 'ChatGPT', 'OpenAI',
    'NVIDIA', 'googleAI', 'microsoftAI', 'anthropic', 'stabilityai'
]

# Fixed paths for new structure
RAW_DIR = os.path.join(os.path.dirname(__file__), '..', 'data', 'raw', 'reddit')
STAGING_DIR = os.path.join(os.path.dirname(__file__), '..', 'data', 'staging')
os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(STAGING_DIR, exist_ok=True)

analyzer = SentimentIntensityAnalyzer()

# Function to validate Reddit credentials
def validate_reddit_credentials():
    if not REDDIT_CLIENT_ID or not REDDIT_CLIENT_SECRET:
        print("ERROR: Reddit client_id/client_secret not found.")
        print("1) Copy .env.example to .env and fill REDDIT_CLIENT_ID and REDDIT_CLIENT_SECRET.")
        print("   Example (.env file contents):")
        print("     REDDIT_CLIENT_ID=your_client_id_here")
        print("     REDDIT_CLIENT_SECRET=your_client_secret_here")
        print("")
        print("2) Or set environment variables in PowerShell: ")
        print("     $Env:REDDIT_CLIENT_ID='your_client_id_here'")
        print("     $Env:REDDIT_CLIENT_SECRET='your_client_secret_here'")
        print("")
        print("After that, re-run: python ingest/ingest_reddit.py")
        return False
    return True

# Initialize Reddit client with error handling and rate limit configuration
try:
    if not validate_reddit_credentials():
        raise SystemExit(1)

    print("Initializing Reddit client...")
    reddit = praw.Reddit(
        client_id=REDDIT_CLIENT_ID,
        client_secret=REDDIT_CLIENT_SECRET,
        user_agent=REDDIT_USER_AGENT,
        ratelimit_seconds=5,  # Wait 5 seconds between requests if we hit the rate limit
        timeout=30  # 30 second timeout for requests
    )
    
    # Test the connection
    reddit.read_only = True  # Ensure we're in read-only mode
    try:
        # Quick test to ensure the client is working
        test_sub = reddit.subreddit('test')
        next(test_sub.new(limit=1), None)  # Use next() to get just one post or None
        print("Reddit client initialized successfully")
    except Exception as e:
        print(f"Error testing Reddit connection: {str(e)}")
        print("Make sure your Reddit API credentials are correct and you have proper network connectivity.")
        raise SystemExit(1)
        
except praw.exceptions.PRAWException as e:
    print(f"Reddit API Error: {str(e)}")
    print("Check your REDDIT_CLIENT_ID and REDDIT_CLIENT_SECRET in .env file")
    raise SystemExit(1)
except Exception as e:
    print(f"Failed to initialize Reddit client: {str(e)}")
    raise SystemExit(1)

def process_subreddit(subreddit, post_type='new', limit=100):
    """Process a single subreddit with proper error handling"""
    try:
        print(f"Processing r/{subreddit} ({post_type})...")
        sub = reddit.subreddit(subreddit)
        posts = []
        
        # Use proper method for post type
        try:
            if post_type == 'top':
                post_iterator = sub.top(limit=limit, time_filter='month')
            else:
                post_iterator = sub.new(limit=limit)
            
            # Convert iterator to list with timeout using a list comprehension
            try:
                posts = list(islice(post_iterator, limit))
                if posts:
                    print(f"Found {len(posts)} posts in r/{subreddit} ({post_type})")
                else:
                    print(f"No posts found in r/{subreddit} ({post_type})")
            except Exception as e:
                print(f"Error getting posts from r/{subreddit} ({post_type}): {str(e)}")
                return []
            
            if not posts:
                return []
                
        except TimeoutError:
            print(f"Timeout while fetching posts from r/{subreddit}")
            return []
        except Exception as e:
            print(f"Error fetching posts from r/{subreddit}: {str(e)}")
            return []
            
        records = []
        for post in posts:
            try:
                created = datetime.fromtimestamp(post.created_utc)
                if created < datetime(2020, 1, 1):
                    continue
                
                text = post.title + ' ' + (post.selftext or '')
                sentiment = analyzer.polarity_scores(text)
                
                # Enhanced sentiment analysis with AI-specific terms
                ai_keywords = ['AI', 'artificial intelligence', 'machine learning', 'neural', 'GPU', 'LLM', 'GPT']
                ai_mentions = sum(1 for keyword in ai_keywords if keyword.lower() in text.lower())
                
                # Determine sentiment intensity and confidence
                sentiment_score = sentiment['compound']
                confidence = max(sentiment['pos'], sentiment['neg'])  # Higher confidence for polarized sentiment
                    
                record = {
                    'subreddit': subreddit,
                    'created_utc': post.created_utc,
                    'title': post.title,
                    'selftext': post.selftext,
                    'score': post.score,
                    'num_comments': post.num_comments,
                    'sentiment': sentiment_score,
                    'sentiment_pos': sentiment['pos'],
                    'sentiment_neg': sentiment['neg'],
                    'sentiment_neu': sentiment['neu'],
                    'sentiment_confidence': confidence,
                    'ai_mentions': ai_mentions,
                    'url': post.url,
                    'engagement_score': post.score + post.num_comments * 0.5,  # Engagement metric
                    'is_controversial': post.controversiality > 0 if hasattr(post, 'controversiality') else False
                }
                records.append(record)
                print(f"Processed post: {post.title[:50]}...")
                
            except Exception as e:
                print(f"Error processing post in r/{subreddit}: {str(e)}")
                continue
                
        return records
        
    except Exception as e:
        print(f"Error processing subreddit r/{subreddit}: {str(e)}")
        return []

def main():
    all_records = []
    total_processed = 0
    
    # Process each subreddit with both top and new posts
    for subreddit in SUBREDDITS:
        subreddit_records = []
        
        # Get both top and new posts with reduced limits to avoid rate limiting
        for post_type, limit in [('top', 5), ('new', 10)]:  # Reduced limits
            try:
                records = process_subreddit(subreddit, post_type, limit)
                if records:
                    subreddit_records.extend(records)
                    all_records.extend(records)
                    total_processed += len(records)
                    print(f"Retrieved {len(records)} {post_type} posts from r/{subreddit}")
                
                # Adaptive delay between requests based on success
                time.sleep(2 if not records else 0.5)  # Longer delay if no posts found
                
            except Exception as e:
                print(f"Error processing r/{subreddit} {post_type}: {str(e)}")
                time.sleep(2)  # Wait longer after an error
                continue
        
        # Save raw JSON for this subreddit
        if subreddit_records:
            raw_path = os.path.join(RAW_DIR, f"{subreddit}.json")
            pd.DataFrame(subreddit_records).to_json(raw_path, orient='records', lines=True)
            print(f"Saved {len(subreddit_records)} records for r/{subreddit}")

    # Save all to Parquet for staging
    if all_records:
        df = pd.DataFrame(all_records)
        
        # Calculate additional metrics for better bubble detection
        df['engagement_rate'] = df['engagement_score'] / (df['score'] + 1)  # Normalized engagement
        df['sentiment_intensity'] = abs(df['sentiment'])  # Absolute sentiment for intensity
        df['ai_sentiment_score'] = df['sentiment'] * df['ai_mentions']  # AI-weighted sentiment
        
        staging_path = os.path.join(STAGING_DIR, "reddit_clean.parquet")
        df.to_parquet(staging_path, index=False)
        print(f"\nSaved {len(all_records)} total cleaned Reddit records to {staging_path}")
        print(f"AI sentiment breakdown:\n{df['ai_sentiment_score'].describe()}")
    else:
        print("\nNo Reddit records found.")

if __name__ == "__main__":
    main()
