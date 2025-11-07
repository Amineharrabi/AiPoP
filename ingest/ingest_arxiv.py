import os
import pandas as pd
import arxiv
from datetime import datetime, timedelta
from tqdm import tqdm
import time

# Fixed paths for new structure
BASE_DIR = os.path.dirname(os.path.dirname(__file__))
RAW_DIR = os.path.join(BASE_DIR, 'data', 'raw', 'arxiv')
STAGING_DIR = os.path.join(BASE_DIR, 'data', 'staging')
os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(STAGING_DIR, exist_ok=True)

# Enhanced search terms for better AI innovation tracking
SEARCH_TERMS = [
    'large language models',
    'transformer architecture',
    'diffusion models',
    'neural networks hardware',
    'GPU acceleration',
    'AI chips',
    'tensor processing unit',
    'neural processing unit',
    'AI accelerator',
    'machine learning compiler',
    'deep learning optimization',
    'AI infrastructure',
    'neural architecture search',
    'neuromorphic computing',
    'quantum machine learning',
    'generative AI',
    'multimodal learning',
    'federated learning',
    'explainable AI',
    'AI safety'
]

def fetch_papers(search_query, max_results=50):
    """Fetch papers from arXiv based on search query with enhanced innovation tracking"""
    try:
        # Create search client
        search = arxiv.Search(
            query=search_query,
            max_results=max_results,
            sort_by=arxiv.SortCriterion.SubmittedDate,
            sort_order=arxiv.SortOrder.Descending
        )

        results = []
        for paper in tqdm(search.results(), desc=f"Fetching {search_query}", leave=False):
            # Include papers from the last 30 days (timezone-aware comparison)
            thirty_days_ago = datetime.now().astimezone() - timedelta(days=30)
            paper_published = paper.published.astimezone() if paper.published.tzinfo else paper.published.replace(tzinfo=thirty_days_ago.tzinfo)
            if paper_published < thirty_days_ago:
                continue

            # Extract authors
            authors = [author.name for author in paper.authors]

            # Extract categories
            categories = [cat for cat in paper.categories]
            
            # Analyze abstract for innovation metrics
            abstract_lower = paper.summary.lower()
            
            # Enhanced Technical Innovation Score Components
            innovation_metrics = {
                'novel_method': any(term in abstract_lower for term in [
                    'novel', 'new approach', 'new method', 'innovative', 'first time',
                    'state-of-the-art', 'sota', 'breakthrough', 'unprecedented'
                ]),
                
                'performance_improvement': any(term in abstract_lower for term in [
                    'outperform', 'better than', 'improvement', 'superior', 
                    'higher accuracy', 'faster', 'more efficient', 'enhanced'
                ]),
                
                'resource_efficiency': any(term in abstract_lower for term in [
                    'efficient', 'reduced memory', 'lower latency', 'fewer parameters',
                    'compressed', 'lightweight', 'optimization', 'sparse'
                ]),
                
                'reproducibility': any(term in abstract_lower for term in [
                    'code available', 'github', 'implementation', 'open source',
                    'reproduce', 'benchmark', 'dataset', 'replication'
                ]),
                
                'practical_application': any(term in abstract_lower for term in [
                    'real-world', 'production', 'deployed', 'industry', 'practical',
                    'application', 'use case', 'commercial', 'scalable'
                ]),
                
                # NEW: AI-specific innovation indicators
                'federated_learning': any(term in abstract_lower for term in [
                    'federated learning', 'distributed learning', 'privacy-preserving'
                ]),
                'multimodal': any(term in abstract_lower for term in [
                    'multimodal', 'vision-language', 'cross-modal', 'audio-visual'
                ]),
                'edge_ai': any(term in abstract_lower for term in [
                    'edge computing', 'on-device', 'mobile AI', 'edge deployment'
                ])
            }
            
            # Calculate enhanced Innovation Score (0-1)
            innovation_score = sum(innovation_metrics.values()) / len(innovation_metrics)
            
            # Calculate Technical Complexity (based on technical terms density)
            technical_terms = [
                'algorithm', 'architecture', 'model', 'neural', 'training',
                'optimization', 'parameter', 'computation', 'inference', 'gpu',
                'processor', 'memory', 'latency', 'throughput', 'benchmark',
                'transformer', 'attention', 'convolution', 'backpropagation'
            ]
            technical_density = sum(term in abstract_lower for term in technical_terms) / len(technical_terms)
            
            # NEW: Calculate Research Impact Score
            impact_factors = {
                'conference_quality': any(conf in abstract_lower for conf in [
                    'neurips', 'icml', 'iclr', 'aaai', 'ijcai', 'cvpr', 'iccv', 'eccv'
                ]),
                'practical_relevance': any(term in abstract_lower for term in [
                    'production', 'real-world', 'deployment', 'industry', 'commercial'
                ]),
                'novel_architecture': any(term in abstract_lower for term in [
                    'new architecture', 'novel design', 'new framework', 'innovative structure'
                ])
            }
            
            research_impact_score = sum(impact_factors.values()) / len(impact_factors)
            
            record = {
                'title': paper.title,
                'abstract': paper.summary,
                'authors': authors,
                'published': paper.published,
                'updated': paper.updated,
                'doi': paper.doi,
                'primary_category': paper.primary_category,
                'categories': categories,
                'pdf_url': paper.pdf_url,
                'entry_id': paper.entry_id,
                'search_term': search_query,
                
                # Enhanced Innovation and Impact Metrics
                'innovation_score': innovation_score,
                'technical_complexity': technical_density,
                'research_impact_score': research_impact_score,
                
                # Individual innovation components
                'has_novel_method': innovation_metrics['novel_method'],
                'has_performance_improvement': innovation_metrics['performance_improvement'],
                'has_resource_efficiency': innovation_metrics['resource_efficiency'],
                'has_reproducibility': innovation_metrics['reproducibility'],
                'has_practical_application': innovation_metrics['practical_application'],
                'has_federated_learning': innovation_metrics['federated_learning'],
                'has_multimodal': innovation_metrics['multimodal'],
                'has_edge_ai': innovation_metrics['edge_ai'],
                
                # Publication metrics
                'publication_impact': len(authors),  # Number of collaborators as a proxy
                'is_high_quality_venue': impact_factors['conference_quality'],
                'has_practical_relevance': impact_factors['practical_relevance'],
                'has_novel_architecture': impact_factors['novel_architecture']
            }
            results.append(record)

            # Brief pause to be nice to arXiv's servers
            time.sleep(0.1)

        return results

    except Exception as e:
        print(f"Error fetching papers for query '{search_query}': {str(e)}")
        return []

def main():
    all_papers = []
    
    # Fetch papers for each search term
    for term in SEARCH_TERMS:
        papers = fetch_papers(term)
        all_papers.extend(papers)
        
        # Save raw data for each term
        if papers:
            term_filename = term.replace(' ', '_').lower()
            raw_file = os.path.join(RAW_DIR, f"{term_filename}.json")
            pd.DataFrame(papers).to_json(raw_file, orient='records', lines=True)
    
    if all_papers:
        # Convert to DataFrame
        df = pd.DataFrame(all_papers)
        
        # Clean and transform data
        df['published'] = pd.to_datetime(df['published'])
        df['updated'] = pd.to_datetime(df['updated'])
        
        # Save combined raw data
        raw_combined = os.path.join(RAW_DIR, 'all_papers.json')
        df.to_json(raw_combined, orient='records', lines=True)
        
        # Save cleaned data to staging with enhanced metrics
        df['innovation_intensity'] = df['innovation_score'] * df['technical_complexity']  # Combined intensity
        df['impact_score'] = df['innovation_score'] * 0.6 + df['research_impact_score'] * 0.4  # Weighted impact
        
        # Ensure timezone awareness for all datetime columns
        df['published'] = df['published'].dt.tz_localize('UTC') if df['published'].dt.tz is None else df['published'].dt.tz_convert('UTC')
        df['updated'] = df['updated'].dt.tz_localize('UTC') if df['updated'].dt.tz is None else df['updated'].dt.tz_convert('UTC')
        df['collected_at'] = datetime.now().astimezone().replace(microsecond=0)
        
        staging_file = os.path.join(STAGING_DIR, 'arxiv_clean.parquet')
        df.to_parquet(staging_file, index=False)
        
        print(f"\nProcessed {len(all_papers)} papers")
        print(f"Average innovation score: {df['innovation_score'].mean():.3f}")
        print(f"Average technical complexity: {df['technical_complexity'].mean():.3f}")
        print(f"Raw data saved to {RAW_DIR}")
        print(f"Cleaned data saved to {staging_file}")
    else:
        print("No papers were found")

if __name__ == "__main__":
    main()