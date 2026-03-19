# SPY Daily News Summaries

Generates a daily plain-text summary of market-moving headlines that may influence `$SPY`, using **public RSS feeds** (no API keys).

## Output
- Files are written to `summaries/YYYY-MM-DD.txt`.

## Run locally
From the repo root:

- Install:
  - `python -m pip install -r spy-daily-news/requirements.txt`
- Generate:
  - `python spy-daily-news/src/daily_spy_news.py --date 2026-03-19`

## Configure feeds
- Edit `spy-daily-news/src/feeds.json`.

