import tweepy, time, re, logging
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
import psycopg2
from datetime import datetime
import html
import unicodedata

logging.basicConfig(level=logging.INFO)

### Twitter Setup ###
def get_tweets(api_key, api_secret, query, since_time):
    auth = tweepy.AppAuthHandler(api_key, api_secret)
    api = tweepy.API(auth)
    tweets = []
    for attempt in range(5):
        try:
            raw = api.search_tweets(q=query, lang='en', since=since_time, count=100, tweet_mode='extended')
            for t in raw:
                tweets.append({
                    "source": "twitter",
                    "text": clean_text(t.full_text),
                    "author": t.user.screen_name,
                    "verified": t.user.verified,
                    "timestamp": t.created_at,
                    "engagement": t.favorite_count + t.retweet_count,
                    "url": f"https://twitter.com/{t.user.screen_name}/status/{t.id}"
                })
            return tweets
        except tweepy.TooManyRequests:
            logging.warning("Rate limited. Sleeping...")
            time.sleep(2 ** attempt)
    return []

def clean_text(text):
    text = html.unescape(text)
    text = unicodedata.normalize("NFKD", text)
    text = re.sub(r"http\S+|@\S+|#\S+", "", text)  # URLs, mentions, hashtags
    text = re.sub(r"[^a-zA-Z0-9\s\$]", "", text)  # Remove special characters
    text = text.replace('$', '')  # Normalize tickers like $AAPL → AAPL
    return text.lower().strip()

### News Scraping ###
def scrape_news(url, timeout=10):
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch()
            page = browser.new_page()
            page.goto(url, timeout=timeout*1000)
            html_content = page.content()
            soup = BeautifulSoup(html_content, "html.parser")
            title = soup.title.text if soup.title else ""
            paragraphs = soup.find_all('p')
            body = " ".join(p.get_text() for p in paragraphs if 'share' not in p.get_text().lower())
            publisher = re.findall(r'https?://(?:www\.)?([^/]+)/?', url)[0]
            return {
                "source": "news",
                "url": url,
                "publisher": publisher,
                "title": clean_text(title),
                "body": clean_text(body),
                "timestamp": datetime.utcnow()
            }
    except PlaywrightTimeout:
        logging.warning(f"Timeout scraping {url}")
        return None

### Database Storage ###
def store_to_timescaledb(records):
    conn = psycopg2.connect("dbname=sentiment user=postgres")
    cur = conn.cursor()
    for r in records:
        cur.execute("""
            INSERT INTO raw_content (source, author, verified, url, title, body, engagement, publisher, timestamp)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            r.get("source"), r.get("author"), r.get("verified"), r.get("url"),
            r.get("title", ""), r.get("body", r.get("text", "")),
            r.get("engagement", 0), r.get("publisher", ""), r.get("timestamp")
        ))
    conn.commit()
    conn.close()
