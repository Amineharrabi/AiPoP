# Quick Start: Deploy to Streamlit Cloud with Hourly Updates

## 🚀 Quick Deployment Steps

### 1. Prepare Your Repository

```bash
# Make sure your database can be committed (already done in .gitignore)
git add .
git commit -m "Prepare for Streamlit Cloud deployment"
git push origin main
```

### 2. Deploy to Streamlit Cloud

1. Go to https://share.streamlit.io
2. Sign in with GitHub
3. Click **"New app"**
4. Select your repository
5. **Main file path**: `run.py`
6. Click **"Deploy!"**

### 3. Enable GitHub Actions

1. Go to your repo → **Settings** → **Actions** → **General**
2. Under **Workflow permissions**: Select **"Read and write permissions"**
3. Save

### 4. Test the Workflow

1. Go to **Actions** tab in your repository
2. Click **"Hourly Pipeline Update"**
3. Click **"Run workflow"** → **"Run workflow"** (manual test)

## ✅ What Happens Next

- **Every hour**: GitHub Actions runs the full pipeline
- **Data ingestion**: Fetches new data from all sources (stocks, Reddit, news, GitHub, etc.)
  - Ingestion scripts are smart: they only fetch data since the last update
- **Data processing**: Computes features, indices, and bubble detection
- **Auto-commit**: Database changes are committed back to the repo
- **Streamlit updates**: Your app automatically shows new data points every hour

## 📊 Viewing Updates

- Check **Actions** tab to see workflow runs
- Check commit history to see database updates
- Your Streamlit app will show new points on the graphs every hour

## 🔧 Troubleshooting

**Workflow not running?**
- Check Actions tab for errors
- Verify workflow permissions are set correctly

**Database not updating?**
- Check if the database file exists in `data/warehouse/`
- Verify `.gitignore` allows the database file

**App not showing new data?**
- Streamlit Cloud may take 1-2 minutes to detect changes
- Try refreshing the app

## 📝 Notes

- First run may take longer (initial data fetch)
- GitHub Actions free tier: 2000 minutes/month
- Database file will grow over time (consider periodic cleanup)

