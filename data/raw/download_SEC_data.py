# download_structured_sec.py
import os, json, time, datetime, re
from sec_api import QueryApi, RenderApi
from tqdm import tqdm

AI_KEYWORDS = [
    'artificial intelligence', 'machine learning', 'deep learning', 'neural network',
    'AI', 'ML', 'generative AI', 'LLM', 'large language model', 'transformer',
    'GPU', 'semiconductor', 'chip', 'data center', 'cloud computing',
    'automation', 'robotics', 'autonomous', 'cognitive computing'
]

# ----------------------------------------------------------------------
# CONFIG
# ----------------------------------------------------------------------
API_KEY   = "ef125a674e0949b8fe6e581af5e6ed0809333b96183ea7e1b65ffa168977beac"          # <<<--- CHANGE THIS
BASE_DIR  = os.path.join(os.path.dirname(__file__), "history", "sec")
os.makedirs(BASE_DIR, exist_ok=True)

START = "2020-01-01"
END   = datetime.datetime.today().strftime("%Y-%m-%d")

FORMS = ["10-Q"]    

tickers = [
    'GOOGL','ASML'
]

# ----------------------------------------------------------------------
# API clients
# ----------------------------------------------------------------------
query_api  = QueryApi(api_key=API_KEY)
render_api = RenderApi(api_key=API_KEY)

# ----------------------------------------------------------------------
# Helper functions
# ----------------------------------------------------------------------
def clean(txt): 
    return " ".join(txt.split()) if txt else ""

def extract_ai_mentions(text):
    """Extract sentences containing AI-related keywords"""
    if not text:
        return []
    
    # Split into sentences (basic)
    sentences = re.split(r'[.!?]+', text)
    
    # Find sentences with AI keywords
    ai_sentences = []
    for sent in sentences:
        sent = sent.strip()
        if sent and any(kw.lower() in sent.lower() for kw in AI_KEYWORDS):
            ai_sentences.append(sent)
    
    return ai_sentences

def extract_market_metrics(text):
    """Extract key financial metrics"""
    metrics = {}
    
    # Revenue patterns (handle both billions and millions)
    rev_patterns = [
        r"revenue.*?\$?\s*([\d,]+\.?\d*)\s*billion",
        r"revenue.*?\$?\s*([\d,]+\.?\d*)\s*million",
        r"revenue.*?\$?\s*([\d,]+\.?\d*)"
    ]
    
    # Net Income patterns
    income_patterns = [
        r"net income.*?\$?\s*([\d,]+\.?\d*)\s*billion",
        r"net income.*?\$?\s*([\d,]+\.?\d*)\s*million",
        r"net income.*?\$?\s*([\d,]+\.?\d*)"
    ]
    
    # EPS patterns
    eps_patterns = [
        r"earnings per share.*?\$?\s*([\d,]+\.?\d*)",
        r"EPS.*?\$?\s*([\d,]+\.?\d*)"
    ]
    
    # Try each pattern
    for pattern in rev_patterns:
        match = re.search(pattern, text, re.I)
        if match:
            metrics['revenue'] = match.group(1).replace(',','')
            break
            
    for pattern in income_patterns:
        match = re.search(pattern, text, re.I)
        if match:
            metrics['net_income'] = match.group(1).replace(',','')
            break
            
    for pattern in eps_patterns:
        match = re.search(pattern, text, re.I)
        if match:
            metrics['eps'] = match.group(1).replace(',','')
            break
    
    return metrics

# ----------------------------------------------------------------------
# MAIN LOOP
# ----------------------------------------------------------------------
for ticker in tqdm(tickers, desc="Tickers"):
    out_file = os.path.join(BASE_DIR, f"{ticker}.json")
    if os.path.exists(out_file):
        tqdm.write(f"{ticker} already done – skipping")
        continue

    records = []

    # ----- 1. Query all filings for the ticker -----
    for form in FORMS:
        q = {
            "query": {
                "query_string": {
                    "query": f'ticker:"{ticker}" AND formType:"{form}" AND filedAt:[{START} TO {END}]'
                }
            },
            "from": 0,
            "size": 200,                     # enough for any ticker in the period
            "sort": [{"filedAt": {"order": "desc"}}]
        }
        try:
            resp = query_api.get_filings(q)
            tqdm.write(f"Query response for {ticker} {form}: {json.dumps(resp, indent=2)[:500]}...")
        except Exception as e:
            tqdm.write(f"{ticker} {form} query error: {e}")
            continue

        filings = resp.get("filings", [])
        if not filings:
            continue

        # ----- 2. Render each filing (plain text) -----
        for f in tqdm(filings, desc=f"{ticker}-{form}", leave=False):
            link = f["linkToFilingDetails"]
            try:
                txt = render_api.get_filing(link)          # plain-text
                time.sleep(0.3)                            # stay inside free-tier limits
            except Exception as e:
                tqdm.write(f"  render error {link[:50]}… {e}")
                txt = ""

            # ----- 3. Extract market-moving sections and AI-related content -----
            rec = {
                "ticker"        : ticker,
                "form"          : f["formType"],
                "filedAt"       : f["filedAt"][:10],
                "periodOfReport": f.get("periodOfReport","")[:10],
                "accessionNo"   : f["accessionNo"],
                "url"          : link,  # Save URL for reference
            }

            # Extract AI-related content from full text
            ai_mentions = extract_ai_mentions(txt)
            if ai_mentions:
                rec["ai_related_content"] = ai_mentions

            # Get financial metrics
            metrics = extract_market_metrics(txt)
            rec.update(metrics)

            # 8-K → Extract relevant items and their content
            if form == "8-K":
                important_items = []
                for item in f.get("items", []):
                    # Focus on material events
                    if any(x in item.lower() for x in [
                        "results of operations",  # Earnings
                        "financial statements",   # Financial updates
                        "material agreement",     # Major deals
                        "regulation fd",          # Important disclosures
                        "changes in control",     # Ownership changes
                        "departure of directors", # Management changes
                        "business acquisition",   # M&A activity
                    ]):
                        important_items.append(item)
                
                if important_items:
                    rec["material_items"] = important_items

            # 10-K / 10-Q → Extract key sections with AI focus
            if form in ("10-K", "10-Q"):
                txt_low = txt.lower()
                
                # Risk Factors (focusing on AI/tech risks)
                risk_start = txt_low.find("item 1a")
                risk_end = txt_low.find("item 2", risk_start)
                if risk_start > -1 and risk_end > -1:
                    risk_text = clean(txt[risk_start:risk_end])
                    ai_risks = extract_ai_mentions(risk_text)
                    if ai_risks:
                        rec["ai_related_risks"] = ai_risks

                # MD&A (with focus on AI/tech initiatives)
                mda_start = txt_low.find("item 7")
                mda_end = txt_low.find("item 8", mda_start)
                if mda_start > -1 and mda_end > -1:
                    mda_text = clean(txt[mda_start:mda_end])
                    ai_initiatives = extract_ai_mentions(mda_text)
                    if ai_initiatives:
                        rec["ai_related_initiatives"] = ai_initiatives

            records.append(rec)

    # ----- 4. Save one JSON per ticker -----
    with open(out_file, "w", encoding="utf-8") as fp:
        json.dump(records, fp, indent=2, ensure_ascii=False)

    tqdm.write(f"{ticker} → {len(records)} records saved")

print("\nAll done! Structured JSONs are in:", BASE_DIR)