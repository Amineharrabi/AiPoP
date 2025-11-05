import os
import pandas as pd
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import time
from tqdm import tqdm

# Load environment variables
load_dotenv()

# Get API key from environment
NEWS_API_KEY = os.getenv('NEWS_API_KEY')
if not NEWS_API_KEY:
    raise ValueError(
        "NEWS_API_KEY not found in environment variables.\n"
        "1) Sign up at https://newsapi.org\n"
        "2) Add to .env file:\n"
        "   NEWS_API_KEY=your_api_key_here"
    )

# Fixed paths for new structure
BASE_DIR = os.path.dirname(os.path.dirname(__file__))
RAW_DIR = os.path.join(BASE_DIR, 'data', 'raw', 'news')
STAGING_DIR = os.path.join(BASE_DIR, 'data', 'staging')
os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(STAGING_DIR, exist_ok=True)

# Companies to track
COMPANIES = [
    'NVIDIA', 'Microsoft', 'Apple', 'Amazon', 'Google', 'Meta', 'Broadcom', 
    'TSMC', 'Oracle', 'Tesla', 'AMD', 'Adobe', 'Salesforce', 'IBM', 'ASML',
    'Intel', 'Qualcomm', 'Micron', 'Applied Materials', 'Lam Research',
    'ServiceNow', 'Palantir', 'Snowflake', 'UiPath', 'C3.ai', 'Synopsys',
    'Cadence', 'ARM Holdings', 'Super Micro Computer', 'Dell'
]

# Enhanced AI-related keywords for better sentiment tracking
AI_KEYWORDS = [
    'artificial intelligence', 'machine learning', 'deep learning', 'neural network',
    'AI', 'ML', 'generative AI', 'LLM', 'large language model', 'transformer',
    'GPU', 'semiconductor', 'chip', 'data center', 'cloud computing',
    'automation', 'computer vision', 'natural language processing', 'robotics'
]

def get_news_articles(query, from_date, to_date):
    """Fetch news articles from NewsAPI"""
    url = 'https://newsapi.org/v2/everything'
    
    params = {
        'q': query,
        'from': from_date,
        'to': to_date,
        'language': 'en',
        'sortBy': 'publishedAt',
        'apiKey': NEWS_API_KEY
    }
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Error fetching news for query '{query}': {str(e)}")
        return None

def main():
    # Initialize sentiment analyzer
    analyzer = SentimentIntensityAnalyzer()
    
    # Get date range (NewsAPI free tier allows last 1 month only)
    end_date = datetime.now()
    start_date = end_date - timedelta(days=28)
    
    all_articles = []
    
    # Get news for each company with enhanced AI tracking
    for company in tqdm(COMPANIES, desc="Fetching company news"):
        # Combine company name with AI keywords for better coverage
        query = f'({company}) AND ({" OR ".join(AI_KEYWORDS)})'
        
        response = get_news_articles(
            query,
            from_date=start_date.strftime('%Y-%m-%d'),
            to_date=end_date.strftime('%Y-%m-%d')
        )
        
        if response and response.get('articles'):
            articles = response['articles']
            
            # Process each article with enhanced sentiment analysis
            for article in articles:
                # Combine title and description for sentiment analysis
                text = f"{article.get('title', '')} {article.get('description', '')}"
                sentiment = analyzer.polarity_scores(text)
                
                # Count AI keyword mentions for weighted sentiment
                ai_mentions = sum(1 for keyword in AI_KEYWORDS if keyword.lower() in text.lower())
                
                # Calculate sentiment intensity and confidence
                sentiment_score = sentiment['compound']
                sentiment_confidence = max(sentiment['pos'], sentiment['neg'])
                
                # Determine article impact based on source and engagement proxy
                source_name = article.get('source', {}).get('name', '')
                impact_score = 1.0  # Base score
                if any(tech_site in source_name.lower() for tech_site in ['tech', 'reuters', 'bloomberg', 'cnbc']):
                    impact_score = 1.5  # Higher impact for major tech/business outlets
                
                record = {
                    'company': company,
                    'publishedAt': article.get('publishedAt'),
                    'title': article.get('title'),
                    'description': article.get('description'),
                    'url': article.get('url'),
                    'source': source_name,
                    
                    # Enhanced sentiment analysis
                    'sentiment': sentiment_score,
                    'sentiment_pos': sentiment['pos'],
                    'sentiment_neg': sentiment['neg'],
                    'sentiment_neu': sentiment['neu'],
                    'sentiment_confidence': sentiment_confidence,
                    
                    # AI-specific metrics
                    'ai_mentions': ai_mentions,
                    'ai_weighted_sentiment': sentiment_score * (1 + ai_mentions * 0.1),  # Boost AI-weighted sentiment
                    
                    # Impact and relevance
                    'impact_score': impact_score,
                    'is_tech_relevant': ai_mentions > 0,
                    
                    # Text analysis
                    'title_length': len(article.get('title', '')),
                    'description_length': len(article.get('description', '')),
                    'has_ai_keywords': ai_mentions > 0
                }
                
                all_articles.append(record)
            
        # Respect API rate limits
        time.sleep(0.1)
    
    if all_articles:
        # Save raw data
        raw_file = os.path.join(RAW_DIR, f"news_{end_date.strftime('%Y%m%d')}.json")
        pd.DataFrame(all_articles).to_json(raw_file, orient='records', lines=True)
        
        # Save cleaned data to staging with enhanced metrics
        df = pd.DataFrame(all_articles)
        df['publishedAt'] = pd.to_datetime(df['publishedAt'])
        
        # Calculate additional metrics for bubble detection
        df['sentiment_intensity'] = abs(df['sentiment'])  # Absolute sentiment for intensity
        df['ai_sentiment_score'] = df['ai_weighted_sentiment']  # AI-weighted sentiment
        df['impact_weighted_sentiment'] = df['sentiment'] * df['impact_score']
        
        staging_file = os.path.join(STAGING_DIR, 'news_clean.parquet')
        df.to_parquet(staging_file, index=False)
        
        print(f"\nProcessed {len(all_articles)} articles")
        print(f"AI-relevant articles: {df['is_tech_relevant'].sum()}")
        print(f"Average AI-weighted sentiment: {df['ai_sentiment_score'].mean():.3f}")
        print(f"Raw data saved to {raw_file}")
        print(f"Cleaned data saved to {staging_file}")
    else:
        print("No articles were found")

if __name__ == "__main__":
    main()