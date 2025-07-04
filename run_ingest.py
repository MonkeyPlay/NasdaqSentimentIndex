# Step 1: Get tweets
tweets = get_tweets(TWITTER_API_KEY, TWITTER_API_SECRET, KEYWORDS, SINCE_TIME)

# Step 2: Scrape news
news = [scrape_news(url) for url in NEWS_URLS]
news = [n for n in news if n]  # Remove failed scrapes

# Step 3: Store in TimescaleDB
store_to_timescaledb(tweets + news)
