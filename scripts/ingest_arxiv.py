import os
import pandas as pd
import arxiv
from datetime import datetime
from tqdm import tqdm
import time

# Setup paths
BASE_DIR = os.path.dirname(os.path.dirname(__file__))
RAW_DIR = os.path.join(BASE_DIR, 'data', 'raw', 'arxiv')
STAGING_DIR = os.path.join(BASE_DIR, 'data', 'staging')
os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(STAGING_DIR, exist_ok=True)

# Search terms for different AI technologies
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
    'quantum machine learning'
]

def fetch_papers(search_query, max_results=100):
    """Fetch papers from arXiv based on search query"""
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
            # Only include papers from 2020 onwards
            if paper.published.year < 2020:
                continue

            # Extract authors
            authors = [author.name for author in paper.authors]

            # Extract categories
            categories = [cat for cat in paper.categories]
            
            # Analyze abstract for innovation metrics
            abstract_lower = paper.summary.lower()
            
            # Technical Innovation Score Components
            innovation_metrics = {
                'novel_method': any(term in abstract_lower for term in [
                    'novel', 'new approach', 'new method', 'innovative', 'first time',
                    'state-of-the-art', 'sota', 'breakthrough'
                ]),
                
                'performance_improvement': any(term in abstract_lower for term in [
                    'outperform', 'better than', 'improvement', 'superior', 
                    'higher accuracy', 'faster', 'more efficient'
                ]),
                
                'resource_efficiency': any(term in abstract_lower for term in [
                    'efficient', 'reduced memory', 'lower latency', 'fewer parameters',
                    'compressed', 'lightweight', 'optimization'
                ]),
                
                'reproducibility': any(term in abstract_lower for term in [
                    'code available', 'github', 'implementation', 'open source',
                    'reproduce', 'benchmark', 'dataset'
                ]),
                
                'practical_application': any(term in abstract_lower for term in [
                    'real-world', 'production', 'deployed', 'industry', 'practical',
                    'application', 'use case'
                ])
            }
            
            # Calculate Innovation Score (0-1)
            innovation_score = sum(innovation_metrics.values()) / len(innovation_metrics)
            
            # Calculate Technical Complexity (based on technical terms density)
            technical_terms = [
                'algorithm', 'architecture', 'model', 'neural', 'training',
                'optimization', 'parameter', 'computation', 'inference', 'gpu',
                'processor', 'memory', 'latency', 'throughput', 'benchmark'
            ]
            technical_density = sum(term in abstract_lower for term in technical_terms) / len(technical_terms)
            
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
                
                # Innovation and Impact Metrics
                'innovation_score': innovation_score,
                'technical_complexity': technical_density,
                'has_novel_method': innovation_metrics['novel_method'],
                'has_performance_improvement': innovation_metrics['performance_improvement'],
                'has_resource_efficiency': innovation_metrics['resource_efficiency'],
                'has_reproducibility': innovation_metrics['reproducibility'],
                'has_practical_application': innovation_metrics['practical_application'],
                'publication_impact': len(authors),  # Number of collaborators as a proxy
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
        
        # Save cleaned data to staging
        staging_file = os.path.join(STAGING_DIR, 'arxiv_clean.parquet')
        df.to_parquet(staging_file, index=False)
        
        print(f"\nProcessed {len(all_papers)} papers")
        print(f"Raw data saved to {RAW_DIR}")
        print(f"Cleaned data saved to {staging_file}")
    else:
        print("No papers were found")

if __name__ == "__main__":
    main()