import os, json, time, datetime, re
from dotenv import load_dotenv
import pandas as pd
from sec_api import QueryApi, RenderApi
from tqdm import tqdm

AI_KEYWORDS = [
    'artificial intelligence', 'machine learning', 'deep learning', 'neural network',
    'AI', 'ML', 'generative AI', 'LLM', 'large language model', 'transformer',
    'GPU', 'semiconductor', 'chip', 'data center', 'cloud computing',
    'automation', 'robotics', 'autonomous', 'cognitive computing'
]

load_dotenv()

API_KEY = os.getenv('SEC_API_KEY', 'YOUR_SEC_API_KEY')
RAW_DIR = os.path.join(os.path.dirname(__file__), '..', 'data', 'raw', 'sec')
STAGING_DIR = os.path.join(os.path.dirname(__file__), '..', 'data', 'staging')
os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(STAGING_DIR, exist_ok=True)

START = "2020-01-01"
END = datetime.datetime.today().strftime("%Y-%m-%d")
FORMS = ["8-K"]
tickers = [
    'NVDA','MSFT','AAPL','AMZN','GOOGL','META','AVGO','TSM','ORCL','TSLA',
    'AMD','ADBE','CRM','IBM','ASML','INTC','QCOM','MU','AMAT','LRCX',
    'NOW','PLTR','SNOW','PATH','AI','SNPS','CDNS','ARM','SMCI','DELL'
]

query_api = QueryApi(api_key=API_KEY)
render_api = RenderApi(api_key=API_KEY)

# Helper functions
def clean(txt):
    return " ".join(txt.split()) if txt else ""

def extract_ai_mentions(text):
    if not text:
        return []
    sentences = re.split(r'[.!?]+', text)
    return [s.strip() for s in sentences if s and any(kw.lower() in s.lower() for kw in AI_KEYWORDS)]

def extract_market_metrics(text):
    metrics = {}
    rev_patterns = [r"revenue.*?\$?\s*([\d,]+\.?\d*)\s*billion", r"revenue.*?\$?\s*([\d,]+\.?\d*)\s*million", r"revenue.*?\$?\s*([\d,]+\.?\d*)"]
    for pattern in rev_patterns:
        match = re.search(pattern, text, re.I)
        if match:
            metrics['revenue'] = match.group(1).replace(',','')
            break
    return metrics

all_records = []
for ticker in tqdm(tickers, desc="Tickers"):
    records = []
    for form in FORMS:
        q = {
            "query": {
                "query_string": {
                    "query": f'ticker:"{ticker}" AND formType:"{form}" AND filedAt:[{START} TO {END}]'
                }
            },
            "from": 0,
            "size": 200,
            "sort": [{"filedAt": {"order": "desc"}}]
        }
        try:
            resp = query_api.get_filings(q)
        except Exception as e:
            tqdm.write(f"{ticker} {form} query error: {e}")
            continue
        filings = resp.get("filings", [])
        if not filings:
            continue
        for f in tqdm(filings, desc=f"{ticker}-{form}", leave=False):
            link = f["linkToFilingDetails"]
            try:
                txt = render_api.get_filing(link)
                time.sleep(0.3)
            except Exception as e:
                tqdm.write(f"  render error {link[:50]}… {e}")
                txt = ""
            rec = {
                "ticker": ticker,
                "form": f["formType"],
                "filedAt": f["filedAt"][:10],
                "accessionNo": f["accessionNo"],
                "url": link,
                "ai_related_content": extract_ai_mentions(txt),
                "metrics": extract_market_metrics(txt)
            }
            records.append(rec)
    # Save raw JSON per ticker
    raw_path = os.path.join(RAW_DIR, f"{ticker}.json")
    with open(raw_path, "w", encoding="utf-8") as fp:
        json.dump(records, fp, indent=2, ensure_ascii=False)
    all_records.extend(records)
    tqdm.write(f"{ticker} → {len(records)} records saved")

# Save all to Parquet for staging
if all_records:
    df = pd.DataFrame(all_records)
    staging_path = os.path.join(STAGING_DIR, "sec_clean.parquet")
    df.to_parquet(staging_path, index=False)
    print(f"Saved cleaned SEC data to {staging_path}")
else:
    print("No SEC records found.")
