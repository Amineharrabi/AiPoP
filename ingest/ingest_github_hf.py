import os
import sys
import pandas as pd
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv
import time
from tqdm import tqdm

# Load environment variables
load_dotenv()

# Get API keys from environment
GITHUB_TOKEN = os.getenv('GITHUB_TOKEN')
HF_TOKEN = os.getenv('HF_ACCESS_TOKEN')

if not GITHUB_TOKEN or not HF_TOKEN:
    print("Warning: Missing API tokens")
    print("For GitHub: Create token at https://github.com/settings/tokens")
    print("For HuggingFace: Create token at https://huggingface.co/settings/tokens")
    print("Add to .env file:")
    print("  GITHUB_TOKEN=your_github_token")
    print("  HF_ACCESS_TOKEN=your_huggingface_token")
    raise SystemExit(1)

# Fixed paths for new structure
BASE_DIR = os.path.dirname(os.path.dirname(__file__))
RAW_DIR = os.path.join(BASE_DIR, 'data', 'raw', 'github_hf')
STAGING_DIR = os.path.join(BASE_DIR, 'data', 'staging')
os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(STAGING_DIR, exist_ok=True)

# Import DataManager for delta calculations
sys.path.append(os.path.join(BASE_DIR, 'setup'))
from data_manager import DataManager

GITHUB_REPOS = [
    # Core Deep Learning Frameworks
    'pytorch/pytorch',
    'tensorflow/tensorflow',
    'google/jax',
    'triton-lang/triton',           
    'keras-team/keras',            

    # Model Libraries & Hubs
    'huggingface/transformers',
    'huggingface/diffusers',
    'huggingface/peft',            
    'huggingface/accelerate',
    'huggingface/datasets',
    'timdettmers/bitsandbytes',    

    # Training Optimization & Scaling
    'microsoft/DeepSpeed',
    'NVIDIA/Megatron-LM',          
    'EleutherAI/gpt-neox',         
    'bigscience-workshop/Megatron-DeepSpeed',
    'facebookresearch/fairscale',  
    'google/maxtext',              

    # Inference Engines & Runtimes
    'NVIDIA/TensorRT',
    'NVIDIA/TensorRT-LLM',         
    'microsoft/onnxruntime',
    'onnx/onnx',
    'pytorch/executorch',          
    'tensorflow/tflite-support',
    'google/mediapipe',            

    # GPU Kernels & Low-Level Acceleration
    'NVIDIA/cutlass',
    'NVIDIA/apex',                 
    'flash-attention/flash-attention',
    'Dao-AILab/flash-attention',   
    'google-research/t5x',         
    'google-research/scenic',      

    # Quantization & Compression
    'intel/neural-compressor',
    'mit-han-lab/aimet',           
    'google/gemmlowp',             
    'nod-ai/SHARK',                
    'tensorflow/model-optimization',

    # Hardware-Specific & Vendor Libraries
    'NVIDIA/cuda-samples',
    'NVIDIA/nccl',                 
    'microsoft/DirectML',
    'oneapi-src/oneDNN',
    'amd/rocBLAS',
    'amd/rocML',                   
    'apple/ml-stable-diffusion',
    'apple/coremltools',
    'google/ai-edge/torch',        
    'google/XNNPACK',              

    # Compilers & MLIR Ecosystem
    'llvm/torch-mlir',             
    'nod-ai/IREEMLIR',             
    'openxla/xla',                 
    'google/iree',                 

    # On-Device & Edge ML
    'pytorch/executorch',
    'pytorch/serve',               
    'google-coral/edgetpu-compiler',
    'raspberrypi/pico-sdk',        
    'tensorflow/tflite-micro',

    # Research & Experimental
    'google-research/google-research',
    'google-deepmind/gemma',       
    'meta-llama/llama',             
    'meta-llama/llama-recipes',
    'karpathy/nanoGPT',            
    'karpathy/llama2.c',           
    'ggerganov/llama.cpp',         
    'ggerganov/whisper.cpp',
    'composable-models/llava',     
    'open-mmlab/mmyolo',
    'open-mmlab/mmdetection',
    'open-mmlab/mmsegmentation',

    # Emerging & Community-Driven
    'vllm-project/vllm',           
    'Tencent/TNN',                 
    'sgl-project/sglang',          
    'guidance-ai/guidance',        
    'lucidrains/x-transformers',
    'lucidrains/PaLM-rlhf-pytorch',
    'state-spaces/mamba',          
    'NVIDIA/TransformerEngine',    
    'pytorch/torchtune',           
    'Lightning-AI/lit-gpt',        
    'bentoml/OpenLLM',             
    'exllama/exllama',             
    'artidoro/qlora',              
    'modal-labs/llm-finetuning'
]

class DeltaCalculator:
    def __init__(self):
        self.data_manager = DataManager()

def get_github_repo_data(repo):
    """Fetch repository data from GitHub API with progress metrics and delta calculations"""
    url = f'https://api.github.com/repos/{repo}'
    headers = {'Authorization': f'token {GITHUB_TOKEN}'}
    
    try:
        # Get repo info
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        repo_data = response.json()
        
        # Get recent commits (last 30 days)
        commits_url = f'{url}/commits'
        since_date = (datetime.now() - timedelta(days=30)).isoformat()
        commits_params = {'since': since_date}
        commits_response = requests.get(commits_url, headers=headers, params=commits_params)
        commits_response.raise_for_status()
        recent_commits = commits_response.json()
        
        # Get recent releases
        releases_url = f'{url}/releases'
        releases_response = requests.get(releases_url, headers=headers)
        releases_response.raise_for_status()
        recent_releases = releases_response.json()
        
        # Get contributor stats
        contributors_url = f'{url}/contributors'
        contributors_response = requests.get(contributors_url, headers=headers)
        contributors_response.raise_for_status()
        contributors = contributors_response.json()
        
        # Calculate activity metrics
        monthly_commit_count = len(recent_commits)
        contributor_count = len(contributors)
        active_contributors = sum(1 for c in contributors if c.get('contributions', 0) > 0)
        
        # Calculate release velocity
        if len(recent_releases) >= 2:
            latest_release_date = datetime.fromisoformat(recent_releases[0]['published_at'].replace('Z', '+00:00'))
            previous_release_date = datetime.fromisoformat(recent_releases[1]['published_at'].replace('Z', '+00:00'))
            days_between_releases = (latest_release_date - previous_release_date).days
            release_frequency = 30 / days_between_releases if days_between_releases > 0 else 0
        else:
            release_frequency = 0
        
        # Prepare current metrics for delta calculation
        current_metrics = {
            'name': repo,
            'stars': repo_data.get('stargazers_count'),
            'forks': repo_data.get('forks_count'),
            'open_issues': repo_data.get('open_issues_count'),
            'watchers': repo_data.get('subscribers_count'),
            'monthly_commits': monthly_commit_count,
            'release_frequency': release_frequency,
            'total_contributors': contributor_count,
            'active_contributors': active_contributors
        }
        
        # Calculate deltas using DataManager
        deltas = DeltaCalculator().data_manager.compute_github_deltas(current_metrics)
        
        # Progress Metrics with enhanced delta tracking
        progress_metrics = {
            # Basic Stats
            'name': repo,
            'stars': repo_data.get('stargazers_count'),
            'forks': repo_data.get('forks_count'),
            'open_issues': repo_data.get('open_issues_count'),
            'watchers': repo_data.get('subscribers_count'),
            
            # Delta Metrics (NEW - for trending analysis)
            'stars_delta': deltas['stars_delta'],
            'forks_delta': deltas['forks_delta'],
            'activity_change': deltas['activity_change'],
            'momentum_score': deltas['momentum_score'],
            
            # Activity Metrics
            'monthly_commits': monthly_commit_count,
            'monthly_release_frequency': release_frequency,
            'total_contributors': contributor_count,
            'active_contributors': active_contributors,
            'contributor_activity_ratio': active_contributors / contributor_count if contributor_count > 0 else 0,
            
            # Community Health
            'stars_per_fork': repo_data.get('stargazers_count', 0) / repo_data.get('forks_count', 1),
            'issues_per_star': repo_data.get('open_issues_count', 0) / repo_data.get('stargazers_count', 1),
            'community_size': repo_data.get('stargazers_count', 0) + repo_data.get('forks_count', 0),
            
            # Project Maturity
            'latest_commit': recent_commits[0]['commit']['committer']['date'] if recent_commits else None,
            'latest_release': recent_releases[0]['published_at'] if recent_releases else None,
            'created_at': repo_data.get('created_at'),
            'updated_at': repo_data.get('updated_at'),
            'project_age_days': (datetime.now() - datetime.fromisoformat(repo_data['created_at'].replace('Z', '+00:00'))).days,
            
            # Technical Details
            'topics': repo_data.get('topics', []),
            'license': repo_data.get('license', {}).get('name'),
            
            # Enhanced Composite Scores (0-1 scale)
            'activity_score': (monthly_commit_count / 100 + release_frequency / 2) / 2,
            'community_score': (active_contributors / 50 + (repo_data.get('stargazers_count', 0) / 10000)) / 2,
            'maturity_score': min(repo_data.get('stargazers_count', 0) / 10000, 1.0),
            
            # NEW: Trending Score based on deltas
            'trending_score': abs(deltas['momentum_score']) + abs(deltas['activity_change']) * 0.5,
            
            'collected_at': datetime.now().astimezone().replace(microsecond=0).isoformat()
        }
        
        return progress_metrics
    
    except Exception as e:
        print(f"Error fetching data for {repo}: {str(e)}")
        return None

def get_huggingface_model_data(model_id):
    """Fetch model data from HuggingFace Hub API with delta calculations"""
    base_url = 'https://huggingface.co/api'
    headers = {'Authorization': f'Bearer {HF_TOKEN}'}
    
    try:
        # Get model info
        model_url = f'{base_url}/models/{model_id}'
        response = requests.get(model_url, headers=headers)
        response.raise_for_status()
        data = response.json()
        
        # Get model card content if available
        try:
            readme_url = f'{base_url}/repos/{model_id}/readme'
            readme_response = requests.get(readme_url, headers=headers)
            readme_response.raise_for_status()
            readme_data = readme_response.json()
            model_card = readme_data.get('raw', {}).get('content', '')
        except:
            model_card = ''
        
        # Get model downloads by month if available
        try:
            stats_url = f'{base_url}/models/{model_id}/stats'
            stats_response = requests.get(stats_url, headers=headers)
            stats_response.raise_for_status()
            stats_data = stats_response.json()
            monthly_downloads = stats_data.get('monthly', {})
        except:
            monthly_downloads = {}
            
        # Prepare current metrics for delta calculation
        current_metrics = {
            'model_id': model_id,
            'downloads': data.get('downloads', 0),
            'likes': data.get('likes', 0),
            'last_modified': data.get('lastModified'),
            'downloads_last_month': monthly_downloads.get(list(monthly_downloads.keys())[-1], 0) if monthly_downloads else 0
        }
        
        # Calculate deltas using DataManager
        deltas = DeltaCalculator().data_manager.compute_hf_deltas(current_metrics)
            
        # Parse timestamps with timezone handling
        def parse_timestamp(ts):
            if not ts:
                return None
            try:
                # Convert to timezone-aware datetime
                return datetime.fromisoformat(ts.replace('Z', '+00:00')).astimezone().replace(microsecond=0)
            except (ValueError, TypeError):
                return None
        
        return {
            'model_id': model_id,
            'name': data.get('name', ''),
            'downloads': data.get('downloads', 0),
            'likes': data.get('likes', 0),
            'tags': data.get('tags', []),
            'pipeline_tag': data.get('pipeline_tag'),
            'last_modified': parse_timestamp(data.get('lastModified')),
            'created_at': parse_timestamp(data.get('createdAt')),
            'author': data.get('author'),
            'license': data.get('license'),
            'private': data.get('private', False),
            
            # NEW: Delta Metrics for trending analysis
            'downloads_delta': deltas['downloads_delta'],
            'likes_delta': deltas['likes_delta'],
            'trending_score': deltas['trending_score'],
            
            'downloads_last_month': monthly_downloads.get(list(monthly_downloads.keys())[-1], 0) if monthly_downloads else 0,
            'model_card': model_card,
            'collected_at': datetime.now().astimezone().replace(microsecond=0)
        }
    
    except Exception as e:
        print(f"Error fetching data for model {model_id}: {str(e)}")
        return None

def discover_ai_models():
    """Dynamically discover active and relevant AI models from HuggingFace"""
    base_url = 'https://huggingface.co/api/models'
    headers = {'Authorization': f'Bearer {HF_TOKEN}'}
    
    # Categories we're interested in
    categories = [
        'text-generation',          # LLMs
        'text-to-image',           # Image generation
        'image-to-text',           # Vision-language
        'text-to-video',           # Video generation
        'image-classification',     # Vision
        'object-detection',        
        'audio-to-text',           # Speech
        'text-to-speech',
        'text-to-audio',
        'feature-extraction',      # Embeddings
        'sentence-similarity',
    ]
    
    discovered_models = set()
    # Create timezone-aware datetime using UTC
    cutoff_date = datetime.now().astimezone().replace(microsecond=0) - timedelta(days=90)
    
    for category in categories:
        try:
            # Sort by recent activity and downloads
            params = {
                'filter': category,
                'sort': 'lastModified',
                'direction': -1,
                'limit': 100,       # Get more results to filter
                'full': True        # Get full metadata
            }
            
            response = requests.get(base_url, headers=headers, params=params)
            response.raise_for_status()
            models = response.json()
            
            for model in models:
                try:
                    # Parse the lastModified timestamp
                    if model.get('lastModified'):
                        last_modified = datetime.fromisoformat(
                            model['lastModified'].replace('Z', '+00:00')
                        )
                    else:
                        continue
                        
                    # Filter for active, well-maintained models
                    if (model.get('downloads', 0) > 1000 and      # Has meaningful adoption
                        not model.get('private', True) and        # Is public
                        last_modified > cutoff_date):             # Updated in last 90 days
                        
                        discovered_models.add(model['modelId'])
                    
                    if len(discovered_models) >= 200:  # Cap total models to analyze
                        break
                except (ValueError, TypeError) as e:
                    print(f"Error processing model {model.get('modelId', 'unknown')}: {str(e)}")
                    continue
            
            time.sleep(0.5)  # Respect rate limits
            
        except Exception as e:
            print(f"Error discovering models for category {category}: {str(e)}")
            continue
    
    return list(discovered_models)

def main():
    
    github_data = []
    for repo in tqdm(GITHUB_REPOS, desc="Fetching GitHub data"):
        data = get_github_repo_data(repo)
        if data:
            github_data.append(data)
        time.sleep(1)  # Respect rate limits
    
    # Save GitHub data
    if github_data:
        df_github = pd.DataFrame(github_data)
        raw_file = os.path.join(RAW_DIR, 'github_repos.json')
        df_github.to_json(raw_file, orient='records', lines=True)
    
    # Collect HuggingFace data
    print("Discovering active AI models...")
    discovered_models = discover_ai_models()
    print(f"Found {len(discovered_models)} relevant AI models")
    
    hf_data = []
    for model_id in tqdm(discovered_models, desc="Fetching HuggingFace data"):
        data = get_huggingface_model_data(model_id)
        if data:
            hf_data.append(data)
        time.sleep(0.5)  # Respect rate limits
    
    # Save HuggingFace data
    if hf_data:
        df_hf = pd.DataFrame(hf_data)
        raw_file = os.path.join(RAW_DIR, 'huggingface_models.json')
        df_hf.to_json(raw_file, orient='records', lines=True)
    
    # Combine and save to staging with enhanced metrics
    if github_data or hf_data:
        # Save GitHub data
        if github_data:
            df_github['source'] = 'github'
            # Add trending classification
            df_github['trending_category'] = df_github['momentum_score'].apply(
                lambda x: 'hot' if x > 0.1 else 'stable' if x > -0.1 else 'declining'
            )
            staging_file = os.path.join(STAGING_DIR, 'github_clean.parquet')
            df_github.to_parquet(staging_file, index=False)
            print(f"\nProcessed {len(github_data)} GitHub repositories")
            print(f"Trending breakdown: {df_github['trending_category'].value_counts().to_dict()}")
            
        # Save HuggingFace data
        if hf_data:
            df_hf['source'] = 'huggingface'
            # Add trending classification
            df_hf['trending_category'] = df_hf['trending_score'].apply(
                lambda x: 'hot' if x > 0.05 else 'stable' if x > -0.05 else 'declining'
            )
            staging_file = os.path.join(STAGING_DIR, 'huggingface_clean.parquet')
            df_hf.to_parquet(staging_file, index=False)
            print(f"Processed {len(hf_data)} HuggingFace models")
            print(f"Trending breakdown: {df_hf['trending_category'].value_counts().to_dict()}")
    else:
        print("No data was collected")

if __name__ == "__main__":
    main()