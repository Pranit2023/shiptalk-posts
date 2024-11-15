import praw
import json
import time
from datetime import datetime

# Reddit API credentials (replace with your own)
CLIENT_ID = 'VMEp44xTVtl0lgUGbjSBcQ'
CLIENT_SECRET = 'wMRISmcP_vxrtXMLqFFW-Mpr0n0uWg'
USER_AGENT = 'RedditScraper:v1.0 (by /u/pranit3112)'

# Initialize Reddit API client
reddit = praw.Reddit(client_id=CLIENT_ID, client_secret=CLIENT_SECRET, user_agent=USER_AGENT)

# Search parameters
SUBREDDITS = ["logistics", "shipping", "supplychain", "freight", "transportation"]
TOPICS_KEYWORDS = {
    "Parcel Shipping": ["parcel shipping", "package shipping", "parcel delivery", "package transit"],
    "Sustainable Packaging": ["eco-friendly packaging", "sustainable packaging", "green packaging"],
    "Last Mile Innovation": ["last mile delivery", "last mile solutions", "final delivery stage"],
    "Integration": ["system integration", "platform integration", "integration with"],
    "Carrier Solutions": ["carrier options", "carrier comparison", "shipping carriers", "freight carriers"],
    "Eco-Friendly": ["eco-friendly", "environmentally friendly", "sustainable", "green"],
    "3-2-1 Shipping": ["3-2-1 shipping", "3-2-1 logistics"],
    "Just-In-Time Inventory": ["just-in-time inventory", "JIT inventory", "inventory management"],
    "Cross-Docking": ["cross-docking", "dock transfer", "direct unloading"],
    "Distributed Inventory": ["distributed inventory", "inventory distribution", "regional inventory"],
    "Last-Mile Delivery Solutions": ["last-mile solutions", "last-mile logistics", "final mile delivery"],
    "Freight Consolidation": ["freight consolidation", "shipment consolidation", "consolidated freight"],
    "Dynamic Routing": ["dynamic routing", "adaptive routing", "route optimization"],
    "Third-Party Logistics (3PL)": ["third-party logistics", "3PL", "outsourced logistics"],
    "Seasonal Planning": ["seasonal planning", "holiday planning", "peak season planning"],
    "Cycle Counting": ["cycle counting", "inventory counting", "inventory auditing"],
    "Sales and Operations Planning (S&OP)": ["sales and operations planning", "S&OP", "sales planning"],
    "Cost-to-Serve Analysis": ["cost-to-serve", "cost analysis", "serve cost analysis"],
}

# Output file for saving results
OUTPUT_FILE = "reddit_shipping_posts.json"

# Helper function to classify posts based on keywords
def classify_post(title, content):
    for category, keywords in TOPICS_KEYWORDS.items():
        for keyword in keywords:
            if keyword.lower() in title.lower() or keyword.lower() in content.lower():
                return category
    return None

# Function to fetch comments for a post
def fetch_comments(submission):
    comments = []
    submission.comments.replace_more(limit=0)
    for comment in submission.comments.list():
        comments.append({
            "id": comment.id,
            "author": str(comment.author),
            "body": comment.body,
            "created_utc": datetime.utcfromtimestamp(comment.created_utc).strftime('%Y-%m-%d %H:%M:%S'),
        })
    return comments

# Function to scrape posts from target subreddits
def scrape_reddit():
    results = []
    for subreddit in SUBREDDITS:
        print(f"Scraping subreddit: {subreddit}")
        for submission in reddit.subreddit(subreddit).new(limit=None):
            post_category = classify_post(submission.title, submission.selftext)
            if post_category:
                post_data = {
                    "id": submission.id,
                    "title": submission.title,
                    "content": submission.selftext,
                    "subreddit": subreddit,
                    "type": "question" if submission.is_self else "discussion",
                    "category": post_category,
                    "created_utc": datetime.utcfromtimestamp(submission.created_utc).strftime('%Y-%m-%d %H:%M:%S'),
                    "comments": fetch_comments(submission),
                }
                results.append(post_data)
            # Respect Reddit's rate limits
            time.sleep(1)
    return results

# Main function to run the scraper and save results
def main():
    try:
        print("Starting Reddit scraper...")
        posts = scrape_reddit()
        print(f"Scraped {len(posts)} relevant posts.")
        
        # Save results to JSON file
        with open(OUTPUT_FILE, 'w') as file:
            json.dump(posts, file, indent=4)
        
        print(f"Data saved to {OUTPUT_FILE}")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
