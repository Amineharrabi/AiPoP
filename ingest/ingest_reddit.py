import os
import pandas as pd
import praw
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from datetime import datetime
from dotenv import load_dotenv

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

# Prefer read-only OAuth flow for scraping public posts.
if REDDIT_USERNAME and REDDIT_PASSWORD:
    reddit = praw.Reddit(
        client_id=REDDIT_CLIENT_ID,
        client_secret=REDDIT_CLIENT_SECRET,
        username=REDDIT_USERNAME,
        password=REDDIT_PASSWORD,
        user_agent=REDDIT_USER_AGENT
    )
else:
    # Read-only OAuth requires client_id and client_secret. If they're missing,
    # print actionable instructions and exit instead of raising a cryptic error.
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
        raise SystemExit(1)

    reddit = praw.Reddit(
        client_id=REDDIT_CLIENT_ID,
        client_secret=REDDIT_CLIENT_SECRET,
        user_agent=REDDIT_USER_AGENT
    )

all_records = []
for subreddit in SUBREDDITS:
    print(f"Scraping r/{subreddit}...")
    subreddit_records = []
    try:
        sub = reddit.subreddit(subreddit)
        # Get top posts (adjust limit as needed) and new posts for better coverage
        for post_type, limit in [('top', 300), ('new', 200)]:
            try:
                for post in sub.get_posts(post_type, limit=limit):
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
                    subreddit_records.append(record)
                    all_records.append(record)
            except Exception as e:
                print(f"Warning: Could not fetch {post_type} posts for r/{subreddit}: {e}")
                continue
                
    except Exception as e:
        print(f"Error scraping r/{subreddit}: {e}")

    # Save raw JSON for this subreddit
    raw_path = os.path.join(RAW_DIR, f"{subreddit}.json")
    if subreddit_records:
        pd.DataFrame(subreddit_records).to_json(raw_path, orient='records', lines=True)

# Save all to Parquet for staging
if all_records:
    df = pd.DataFrame(all_records)
    
    # Calculate additional metrics for better bubble detection
    df['engagement_rate'] = df['engagement_score'] / (df['score'] + 1)  # Normalized engagement
    df['sentiment_intensity'] = abs(df['sentiment'])  # Absolute sentiment for intensity
    df['ai_sentiment_score'] = df['sentiment'] * df['ai_mentions']  # AI-weighted sentiment
    
    staging_path = os.path.join(STAGING_DIR, "reddit_clean.parquet")
    df.to_parquet(staging_path, index=False)
    print(f"Saved {len(all_records)} cleaned Reddit records to {staging_path}")
    print(f"AI sentiment breakdown: {df['ai_sentiment_score'].describe()}")
else:
    print("No Reddit records found.")
