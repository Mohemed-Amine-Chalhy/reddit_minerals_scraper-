import requests
import base64
import praw
from datetime import datetime
import json
import os
import time

client_id = 'wSsnfwGHoQBdUyYb1nPjLg'
client_secret = '2H1JM43tPBTjJHBMF7E05oIHOG8tAg'
username = 'SufficientMenu6638'
password = 'mt8qzquL6w6_47W'

reddit = praw.Reddit(
    client_id=client_id,
    client_secret=client_secret,
    user_agent='Metals by SufficientMenu6638',
    username=username,
    password=password
)

reddit.read_only = True

def safe_get_author_id(author):
    try:
        return author.id
    except:
        return None
    
def extract_post_data(submission):
    """Extract minimal post data for text analysis"""
    return {
        'id': submission.id,
        'title': submission.title,
        'selftext': submission.selftext,
        'subreddit': submission.subreddit.display_name,
        'created_utc': submission.created_utc,
        'created_date': datetime.fromtimestamp(submission.created_utc).isoformat(),
        'score': submission.score,
        'num_comments': submission.num_comments,
        'permalink': f"https://reddit.com{submission.permalink}"
    }
    
def extract_comment_data(comment, post_id, subreddit_name, level=0):
    """Extract minimal comment data for text analysis"""
    if hasattr(comment, 'body'):
        return {
            'id': comment.id,
            'post_id': post_id,
            'parent_id': comment.parent_id,
            'author': str(comment.author) if comment.author else '[deleted]',
            'body': comment.body,
            'score': comment.score,
            'created_utc': comment.created_utc,
            'created_date': datetime.fromtimestamp(comment.created_utc).isoformat(),
            'level': level,
            'subreddit': subreddit_name,
            'permalink': f"https://reddit.com{comment.permalink}"
        }
    return None

def load_existing_data(mineral_dir):
    """Load existing posts and comments to avoid duplicates"""
    existing_posts = {}
    existing_comments = {}
    processed_posts = set()
    
    posts_file = f"{mineral_dir}/posts.json"
    comments_file = f"{mineral_dir}/comments.json"
    progress_file = f"{mineral_dir}/progress.json"
    
    if os.path.exists(posts_file):
        with open(posts_file, 'r', encoding='utf-8') as f:
            posts_data = json.load(f)
            for post in posts_data:
                existing_posts[post['id']] = post
    
    if os.path.exists(comments_file):
        with open(comments_file, 'r', encoding='utf-8') as f:
            comments_data = json.load(f)
            for comment in comments_data:
                existing_comments[comment['id']] = comment
    
    if os.path.exists(progress_file):
        with open(progress_file, 'r', encoding='utf-8') as f:
            progress = json.load(f)
            processed_posts = set(progress.get('processed_posts', []))
    
    return existing_posts, existing_comments, processed_posts

def save_progress(mineral_dir, processed_posts):
    """Save progress to resume later"""
    progress_file = f"{mineral_dir}/progress.json"
    progress = {
        'processed_posts': list(processed_posts),
        'last_updated': datetime.now().isoformat()
    }
    with open(progress_file, 'w', encoding='utf-8') as f:
        json.dump(progress, f, indent=2)

def save_data(mineral_dir, posts_data, comments_data):
    """Save posts and comments data"""
    with open(f"{mineral_dir}/posts.json", "w", encoding="utf-8") as f:
        json.dump(list(posts_data.values()), f, indent=2, ensure_ascii=False)

    with open(f"{mineral_dir}/comments.json", "w", encoding="utf-8") as f:
        json.dump(list(comments_data.values()), f, indent=2, ensure_ascii=False)

def get_all_comments(submission, post_id, existing_comments):
    """Recursively get all comments and replies, skipping existing ones"""
    new_comments = {}
    
    try:
        submission.comments.replace_more(limit=None)
        subreddit_name = submission.subreddit.display_name

        def process_comment_tree(comment, level=0):
            if hasattr(comment, 'id') and comment.id not in existing_comments:
                comment_data = extract_comment_data(comment, post_id, subreddit_name, level)
                if comment_data:
                    new_comments[comment.id] = comment_data
                    
            # Process replies regardless of whether parent was already processed
            if hasattr(comment, 'replies'):
                for reply in comment.replies:
                    process_comment_tree(reply, level + 1)

        for comment in submission.comments:
            process_comment_tree(comment)
            
    except Exception as e:
        print(f"      ❌ Failed to get comments: {e}")
    
    return new_comments

# Load subreddit mapping
with open('configs\subreddit_mapping.json') as f:
    mineral_subreddits = json.load(f)

os.makedirs('data', exist_ok=True)

for mineral, subreddits in mineral_subreddits.items():
    print(f"\n🔍 Scraping for '{mineral}'...")
    
    # Create directory for this mineral
    mineral_dir = f"data/{mineral}"
    os.makedirs(mineral_dir, exist_ok=True)

    # Load existing data
    existing_posts, existing_comments, processed_posts = load_existing_data(mineral_dir)
    
    print(f"  📊 Found {len(existing_posts)} existing posts, {len(existing_comments)} existing comments")
    print(f"  📊 {len(processed_posts)} posts already fully processed")

    new_posts_count = 0
    new_comments_count = 0

    for sub in subreddits:
        print(f"  📂 Searching in r/{sub}...")
        subreddit = reddit.subreddit(sub)
        
        try:
            for submission in subreddit.search(mineral, time_filter='all', limit=None):
                # Skip if post already exists and was fully processed
                if submission.id in processed_posts:
                    print(f"    ⏭️  Skipping already processed post: {submission.title}")
                    continue
                
                # Add or update post data
                if submission.id not in existing_posts:
                    print(f"    📝 New post: {submission.title}")
                    post_data = extract_post_data(submission)
                    existing_posts[submission.id] = post_data
                    new_posts_count += 1
                else:
                    print(f"    🔄 Updating post: {submission.title}")

                # Get comments for this post
                print(f"      💬 Getting comments...")
                new_comments = get_all_comments(submission, submission.id, existing_comments)
                
                # Add new comments to existing comments
                existing_comments.update(new_comments)
                new_comments_count += len(new_comments)
                
                # Mark this post as processed
                processed_posts.add(submission.id)
                
                print(f"      ✅ Found {len(new_comments)} new comments")
                
                # Save progress periodically
                if len(processed_posts) % 10 == 0:  # Save every 10 posts
                    save_data(mineral_dir, existing_posts, existing_comments)
                    save_progress(mineral_dir, processed_posts)
                    print(f"      💾 Progress saved...")
                
                time.sleep(1)  # Rate limiting
                
        except Exception as e:
            print(f"  ❌ Failed to search r/{sub}: {e}")

    # Final save
    save_data(mineral_dir, existing_posts, existing_comments)
    save_progress(mineral_dir, processed_posts)

    # Create summary
    posts_list = list(existing_posts.values())
    comments_list = list(existing_comments.values())
    
    summary = {
        'extraction_date': datetime.now().isoformat(),
        'total_posts': len(posts_list),
        'total_comments': len(comments_list),
        'new_posts_this_run': new_posts_count,
        'new_comments_this_run': new_comments_count,
        'subreddits_searched': subreddits,
        'search_query': mineral,
        'posts_by_subreddit': {},
        'comments_by_subreddit': {}
    }

    for post in posts_list:
        subreddit = post['subreddit']
        summary['posts_by_subreddit'][subreddit] = summary['posts_by_subreddit'].get(subreddit, 0) + 1

    for comment in comments_list:
        subreddit = comment['subreddit']
        summary['comments_by_subreddit'][subreddit] = summary['comments_by_subreddit'].get(subreddit, 0) + 1

    with open(f"{mineral_dir}/summary.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)

    print(f"✅ Completed scraping for '{mineral}':")
    print(f"   📊 Total: {len(posts_list)} posts, {len(comments_list)} comments")
    print(f"   🆕 New this run: {new_posts_count} posts, {new_comments_count} comments")