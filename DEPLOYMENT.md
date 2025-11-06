# Deployment Guide for Streamlit Community Cloud

This guide explains how to deploy the AI Bubble Detection dashboard to Streamlit Community Cloud with automated hourly updates.

## Prerequisites

1. A GitHub account
2. A Streamlit Community Cloud account (free)
3. API keys for data sources (if required):
   - Reddit API (optional, for Reddit data)
   - SEC API (optional, for SEC filings)
   - News API (optional, for news data)

## Step 1: Prepare Your Repository

1. **Update .gitignore**: The database file needs to be committed for Streamlit Cloud to access it. Update your `.gitignore`:

```bash
# Comment out or remove this line:
# data/warehouse/**
```

Or create a specific exception:
```
data/warehouse/ai_bubble.duckdb
!data/warehouse/.gitkeep
```

2. **Ensure all files are committed**:
```bash
git add .
git commit -m "Prepare for Streamlit Cloud deployment"
git push
```

## Step 2: Set Up GitHub Secrets (Optional)

If your pipeline requires API keys, add them as GitHub Secrets:

1. Go to your repository on GitHub
2. Navigate to **Settings** → **Secrets and variables** → **Actions**
3. Add the following secrets (if needed):
   - `REDDIT_CLIENT_ID`
   - `REDDIT_CLIENT_SECRET`
   - `SEC_API_KEY`
   - `NEWS_API_KEY`

## Step 3: Deploy to Streamlit Community Cloud

1. Go to [share.streamlit.io](https://share.streamlit.io)
2. Sign in with your GitHub account
3. Click **"New app"**
4. Select your repository
5. Set the **Main file path** to: `run.py`
6. Click **"Deploy!"**

The app will be available at: `https://your-app-name.streamlit.app`

## Step 4: Enable GitHub Actions

The hourly pipeline updates are handled by GitHub Actions:

1. Go to your repository on GitHub
2. Navigate to **Settings** → **Actions** → **General**
3. Under **Workflow permissions**, select **"Read and write permissions"**
4. Check **"Allow GitHub Actions to create and approve pull requests"**

## Step 5: Verify Hourly Updates

1. The GitHub Actions workflow (`.github/workflows/hourly_pipeline.yml`) runs automatically every hour
2. You can manually trigger it from the **Actions** tab in your repository
3. The workflow will:
   - Run the full pipeline (with `--full-refresh`) to ensure all stages execute
   - **Ingestion stages** fetch new data from all sources (yfinance, Reddit, news, etc.)
     - Note: Ingestion scripts handle incremental fetching internally (only get new data since last update)
   - **Processing stages** compute features, indices, and detect bubbles
   - Update the database with new data
   - Commit and push the updated database back to the repository
   - Streamlit Cloud will automatically detect the changes and update the app

## Monitoring

- **GitHub Actions**: Check the **Actions** tab to see workflow runs and logs
- **Streamlit Cloud**: Check the app logs in the Streamlit Cloud dashboard
- **Database Updates**: Check the commit history to see when the database was last updated

## Troubleshooting

### Database not updating
- Check GitHub Actions logs for errors
- Ensure the workflow has write permissions
- Verify the database file is being committed (check `.gitignore`)

### App not showing new data
- Streamlit Cloud may take a few minutes to detect changes
- Try refreshing the app or checking the logs
- Verify the database file was successfully committed

### Pipeline errors
- Check the GitHub Actions logs
- Ensure all API keys are set correctly in GitHub Secrets
- Verify all dependencies are in `requirements.txt`

## Manual Pipeline Run

To manually trigger a pipeline update:

1. Go to **Actions** → **Hourly Pipeline Update**
2. Click **"Run workflow"**
3. Select the branch and click **"Run workflow"**

Or from command line:
```bash
python -m setup.run_pipeline --direct
```

## Notes

- The database file will grow over time. Consider periodic cleanup or archiving old data
- GitHub Actions has usage limits on free accounts (2000 minutes/month)
- Streamlit Cloud free tier has resource limits
- The pipeline runs in GitHub Actions, not on Streamlit Cloud servers

