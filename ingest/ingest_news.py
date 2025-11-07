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
        'pageSize': 100,  # Maximum page size
        'apiKey': NEWS_API_KEY
    }
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        time.sleep(1)
        data = response.json()
        
        if data.get('status') != 'ok':
            print(f"API Error for query '{query}': {data.get('message', 'Unknown error')}")
            return None
            
        if not data.get('articles'):
            print(f"No articles found for query '{query}'")
            return None
            
        print(f"Found {len(data['articles'])} articles for query '{query}'")
        return data
        
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 429:  # Rate limit
            print("Rate limit hit, waiting 1 second...")
            time.sleep(1)
            return get_news_articles(query, from_date, to_date)  # Retry
        print(f"HTTP Error fetching news for query '{query}': {str(e)}")
        return None
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
    total_processed = 0
    
    # Get news for each company with enhanced AI tracking
    for company in tqdm(COMPANIES, desc="Fetching company news"):
        # Combine company name with AI keywords for better coverage
        query = f'({company}) AND ({" OR ".join(AI_KEYWORDS)})'
        
        try:
            response = get_news_articles(
                query,
                from_date=start_date.strftime('%Y-%m-%d'),
                to_date=end_date.strftime('%Y-%m-%d')
            )
            
            if not response:
                print(f"No response for {company}")
                continue
            
            articles = response.get('articles', [])
            if not articles:
                print(f"No articles found for {company}")
                continue
            
            print(f"Processing {len(articles)} articles for {company}")
            company_articles = []
            
            # Process each article with enhanced sentiment analysis
            for article in articles:
                title = article.get('title', '').strip()
                description = article.get('description', '').strip()
                
                # Skip articles with no content
                if not title and not description:
                    continue
                
                # Combine title and description for sentiment analysis
                text = f"{title} {description}"
                sentiment = analyzer.polarity_scores(text)
                
                # Count AI keyword mentions
                ai_mentions = sum(1 for keyword in AI_KEYWORDS if keyword.lower() in text.lower())
                
                # Skip articles with no AI relevance
                if ai_mentions == 0:
                    continue
                
                # Calculate metrics
                sentiment_score = sentiment['compound']
                sentiment_confidence = max(sentiment['pos'], sentiment['neg'])
                
                # Determine article impact
                source_name = article.get('source', {}).get('name', '')
                impact_score = 1.5 if any(site in source_name.lower() 
                                        for site in ['tech', 'reuters', 'bloomberg', 'cnbc']) else 1.0
                
                record = {
                    'company': company,
                    'publishedAt': article.get('publishedAt'),
                    'title': title,
                    'description': description,
                    'url': article.get('url'),
                    'source': source_name,
                    'sentiment': sentiment_score,
                    'sentiment_pos': sentiment['pos'],
                    'sentiment_neg': sentiment['neg'],
                    'sentiment_neu': sentiment['neu'],
                    'sentiment_confidence': sentiment_confidence,
                    'ai_mentions': ai_mentions,
                    'ai_weighted_sentiment': sentiment_score * (1 + ai_mentions * 0.1),
                    'impact_score': impact_score,
                    'is_tech_relevant': True,
                    'title_length': len(title),
                    'description_length': len(description),
                    'has_ai_keywords': True
                }
                
                company_articles.append(record)
                total_processed += 1
            
            if company_articles:
                print(f"Found {len(company_articles)} relevant articles for {company}")
                all_articles.extend(company_articles)
            
        except Exception as e:
            print(f"Error processing company {company}: {str(e)}")
            continue
        
        # Respect API rate limits
        time.sleep(3)
    
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