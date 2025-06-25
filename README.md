# Reddit Scraper for Metals-related Public Opinion Mining

This Python script extracts **Reddit posts and comments** related to various **metals and minerals** from specified subreddits using the [PRAW](https://praw.readthedocs.io/) API.

---

## 📦 Features

* 🔍 Searches Reddit for posts mentioning a specific mineral across selected subreddits.
* 🧵 Extracts all top-level and nested comments for each post.
* 🧠 Designed for **text analysis**, with clean and structured output.
* ♻️ Avoids duplicates by tracking previously processed posts and comments.
* 💾 Saves data incrementally to allow resuming.
* 📈 Generates a per-mineral summary report with metadata.

---

## 🛠️ Requirements

* Python 3.7+
* Reddit API credentials (via [Reddit App](https://www.reddit.com/prefs/apps))
* Python packages:

  ```bash
  pip install praw requests
  ```

---

## 📁 Project Structure

```
.
├── main_scraper.py               # This script (provided above)
├── configs/
│   └── subreddit_mapping.json    # Mapping of minerals to subreddits
├── data/
│   └── <mineral_name>/           # Folder per mineral with posts/comments
│       ├── posts.json
│       ├── comments.json
│       ├── summary.json
│       └── progress.json
└── README.md
```

---

## 🧩 Input: Subreddit Mapping

You need to create a file at `configs/subreddit_mapping.json` that maps each mineral to relevant subreddits. Example:

```json
{
  "bauxite": ["mining", "environment", "geology"],
  "lithium": ["electricvehicles", "batteries", "renewableenergy"]
}
```

---

## 🚀 How to Use

1. **Set your Reddit API credentials** inside `main_scraper.py`:

   ```python
   client_id = 'YOUR_CLIENT_ID'
   client_secret = 'YOUR_CLIENT_SECRET'
   username = 'YOUR_REDDIT_USERNAME'
   password = 'YOUR_REDDIT_PASSWORD'
   ```

2. **Run the script**:

   ```bash
   python main_scraper.py
   ```

3. The script will:

   * Search each mineral in the associated subreddits.
   * Collect all matching posts and comments.
   * Store data in `data/<mineral>/`.
   * Track progress so it can resume later without duplicating work.

---

## 🧠 Output Example

* `posts.json`: List of all collected posts with metadata (ID, title, body, score, etc.).
* `comments.json`: List of all comments with nested structure and levels.
* `progress.json`: Stores already processed post IDs for incremental scraping.
* `summary.json`: Contains metadata like total posts, comments, and breakdown by subreddit.

---

## ✅ Benefits

* Great for **NLP and sentiment analysis** on public opinion.
* Flexible architecture supports many minerals and subreddits.
* Lightweight and restartable.

---

## 📌 Notes
* Reddit API has rate limits — the script includes a `time.sleep(1)` to prevent hitting them too fast.
* Ensure your `subreddit_mapping.json` is well-formatted and includes relevant, active subreddits.

---


## 👨‍💻 Author

This project was developed by `Mohamed Amine CHALHY` for data collection and analysis related to public discourse around mineral extraction.

---
