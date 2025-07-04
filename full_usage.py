def main():
    api_key = "your_key"
    api_secret = "your_secret"
    tweets = get_tweets(api_key, api_secret, "Nasdaq OR AAPL", "2025-04-30")
    
    urls = ["https://www.cnbc.com/2025/04/30/example-article.html"]
    articles = [scrape_news(u) for u in urls if u]
    
    combined = tweets + [a for a in articles if a]
    store_to_timescaledb(combined)

if __name__ == "__main__":
    main()
