import json
import os
import time
from datetime import datetime
import google.generativeai as genai
from typing import Dict, Any, Optional, List, Tuple
import threading
from queue import Queue
import re
import logging
from dataclasses import dataclass
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configure logging to use UTF-8 encoding
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    encoding='utf-8',
    handlers=[
        logging.FileHandler('enrichment.log', encoding='utf-8'),
        logging.StreamHandler()
    ],
    force=True
)
logger = logging.getLogger(__name__)

# --- No changes to ProcessingStats or EfficientRateLimiter ---
@dataclass
class ProcessingStats:
    """Track processing statistics for a given run."""
    total_processed: int = 0
    successful: int = 0
    failed: int = 0
    blocked: int = 0
    skipped: int = 0
    lock: threading.Lock = threading.Lock()

    def increment(self, successful: bool = False, failed: bool = False, blocked: bool = False):
        with self.lock:
            self.total_processed += 1
            if successful: self.successful += 1
            if failed: self.failed += 1
            if blocked: self.blocked += 1
            
    def add_skipped(self, count: int):
        with self.lock:
            self.skipped += count

    @property
    def success_rate(self) -> float:
        total_attempted = self.successful + self.failed + self.blocked
        return (self.successful / max(total_attempted, 1)) * 100

class EfficientRateLimiter:
    """Manages API calls efficiently within RPM and TPM quotas for concurrent environments."""
    def __init__(self, rpm_limit: int = 800, tpm_limit: int = 900000):
        self.rpm_limit = rpm_limit * 0.9
        self.tpm_limit = tpm_limit * 0.9
        self.request_timestamps = []
        self.token_counts = []
        self.lock = threading.Lock()
        logger.info(f"Rate limiter configured with safety margins: RPM <= {self.rpm_limit}, TPM <= {self.tpm_limit}")

    def wait_if_needed(self, estimated_tokens: int = 1500):
        with self.lock:
            while True:
                current_time = time.time()
                one_minute_ago = current_time - 60
                self.request_timestamps = [t for t in self.request_timestamps if t > one_minute_ago]
                self.token_counts = [tc for tc in self.token_counts if tc['timestamp'] > one_minute_ago]
                
                if len(self.request_timestamps) >= self.rpm_limit:
                    oldest_request_time = self.request_timestamps[0]
                    wait_time = 60 - (current_time - oldest_request_time) + 0.1
                    logger.warning(f"RPM limit approaching. Waiting for {wait_time:.2f}s")
                    time.sleep(wait_time)
                    continue

                current_tpm = sum(tc['tokens'] for tc in self.token_counts)
                if current_tpm + estimated_tokens >= self.tpm_limit:
                    oldest_token_time = self.token_counts[0]['timestamp']
                    wait_time = 60 - (current_time - oldest_token_time) + 0.1
                    logger.warning(f"TPM limit approaching. Waiting for {wait_time:.2f}s")
                    time.sleep(wait_time)
                    continue

                self.request_timestamps.append(current_time)
                self.token_counts.append({'timestamp': current_time, 'tokens': estimated_tokens})
                break

class RedditMiningEnricher:
    """Enriches Reddit data with AI-driven analysis using the Gemini API."""
    
    def __init__(self, config_path: str = "configs/keys.json", num_workers: int = 10):
        self.config = self._load_config(config_path)
        self.rate_limiter = EfficientRateLimiter()
        self._configure_gemini()
        self.num_workers = num_workers
        logger.info(f"Concurrency level set to {self.num_workers} workers.")
        
        self.default_analysis = {
            "sentiment": "Neutral", "keywords": [], "themes": [],
            "concerns_detected": {
                "environment": 0.0, "health": 0.0, "working conditions": 0.0,
                "child labor": 0.0, "pollution": 0.0, "deforestation": 0.0,
                "biodiversity loss": 0.0, "water contamination": 0.0,
                "air quality": 0.0, "government policy": 0.0, "corruption": 0.0,
                "economic benefits": 0.0, "local employment": 0.0,
                "displacement": 0.0, "community rights": 0.0,
                "indigenous rights": 0.0, "waste management": 0.0,
                "foreign exploitation": 0.0, "sustainability": 0.0,
                "safety regulations": 0.0
            },
            "mining_stance": "Neutral", "topic_classification": "neutral"
        }
    
    # --- No changes to helper methods (_load_config through create_blocked_analysis) ---
    def _load_config(self, config_path: str) -> Dict:
        """Load configuration from a JSON file."""
        try:
            with open(config_path, 'r') as f:
                config = json.load(f)
            if not isinstance(config, list) or not config or 'aistudio' not in config[0] or 'apiKey' not in config[0]['aistudio']:
                raise ValueError("Config file is missing expected structure: [{'aistudio': {'apiKey': '...'}}]")
            return config
        except FileNotFoundError:
            logger.error(f"Configuration file not found at: {config_path}")
            raise
        except (json.JSONDecodeError, ValueError) as e:
            logger.error(f"Error parsing config file {config_path}: {e}")
            raise
    
    def _configure_gemini(self) -> None:
        """Configure the Gemini AI client."""
        try:
            api_key = self.config[0]['aistudio']['apiKey']
            genai.configure(api_key=api_key)
            logger.info("Gemini AI configured successfully.")
        except Exception as e:
            logger.error(f"Failed to configure Gemini AI: {e}")
            raise
    
    def estimate_tokens(self, text: str) -> int:
        """Estimate the token count for a given string."""
        if not text:
            return 0
        return len(text) // 4

    def is_content_relevant(self, text: str, mineral: str) -> bool:
        return text and len(text.strip()) >= 15

    def clean_text(self, text: str) -> str:
        if not text:
            return ""
        text = re.sub(r'http[s]?://\S+', '', text)
        text = re.sub(r'\s+', ' ', text)
        text = re.sub(r'\[deleted\]|\[removed\]', '', text, flags=re.IGNORECASE)
        return text.strip()[:3000]
    
    def create_analysis_prompt(self, mineral: str, title: str = "", body: str = "") -> str:
        content = f"Title: {self.clean_text(title)}\n\nBody: {self.clean_text(body)}".strip()
        
        return f"""Analyze the following text about {mineral} mining. Your response MUST be a single, valid JSON object with the exact structure shown below. Do not add any explanatory text or markdown formatting.

{{
  "sentiment": "Positive|Negative|Neutral",
  "keywords": ["keyword1", "keyword2", "keyword3", "keyword4", "keyword5"],
  "themes": ["primary_theme", "secondary_theme"],
  "concerns_detected": {{
    "environment": 0.0, "health": 0.0, "working conditions": 0.0, "child labor": 0.0,
    "pollution": 0.0, "deforestation": 0.0, "biodiversity loss": 0.0, "water contamination": 0.0,
    "air quality": 0.0, "government policy": 0.0, "corruption": 0.0, "economic benefits": 0.0,
    "local employment": 0.0, "displacement": 0.0, "community rights": 0.0, "indigenous rights": 0.0,
    "waste management": 0.0, "foreign exploitation": 0.0, "sustainability": 0.0, "safety regulations": 0.0
  }},
  "mining_stance": "Pro-mining|Anti-mining|Neutral",
  "topic_classification": "mining-related|environmental|economic|social|technical|other"
  "relevance_to_{mineral}_score": 0.0
}}

Text to analyze:
---
{content}
---
"""

    def extract_json_from_response(self, response_text: str) -> Optional[str]:
        if not response_text:
            return None
        match = re.search(r'\{.*\}', response_text, re.DOTALL)
        return match.group(0) if match else None
        
    def create_blocked_analysis(self, reason: str) -> Dict[str, Any]:
        analysis = self.default_analysis.copy()
        analysis["keywords"] = ["content_issue", reason]
        analysis["themes"] = ["content_blocked"]
        analysis["topic_classification"] = "blocked"
        return analysis

    def analyze_content(self, prompt: str, max_retries: int = 3) -> Optional[Dict[str, Any]]:
        estimated_tokens = self.estimate_tokens(prompt) + 600
        self.rate_limiter.wait_if_needed(estimated_tokens)
        
        model = genai.GenerativeModel('gemini-1.5-flash-latest')
        
        for attempt in range(max_retries):
            try:
                response = model.generate_content(
                    prompt,
                    generation_config={"temperature": 0.1, "max_output_tokens": 800},
                    safety_settings=[{"category": c, "threshold": "BLOCK_NONE"} for c in ["HARM_CATEGORY_HARASSMENT", "HARM_CATEGORY_HATE_SPEECH", "HARM_CATEGORY_SEXUALLY_EXPLICIT", "HARM_CATEGORY_DANGEROUS_CONTENT"]]
                )
                
                if not response.candidates:
                    if hasattr(response, 'prompt_feedback') and response.prompt_feedback.block_reason:
                        logger.warning(f"Prompt blocked by API: {response.prompt_feedback.block_reason_message}")
                        return self.create_blocked_analysis("prompt_blocked")
                    logger.warning(f"No candidates in response (attempt {attempt + 1})")
                    if attempt < max_retries - 1: time.sleep(5); continue
                    return self.create_blocked_analysis("no_candidates")

                candidate = response.candidates[0]
                if candidate.finish_reason.name not in ["STOP", "MAX_TOKENS"]:
                    logger.warning(f"Response blocked by safety filter: {candidate.finish_reason.name}")
                    return self.create_blocked_analysis(f"safety_blocked_{candidate.finish_reason.name.lower()}")
                
                json_text = self.extract_json_from_response(response.text)
                if not json_text:
                    logger.warning(f"No JSON found in response (attempt {attempt + 1})")
                    if attempt < max_retries - 1: time.sleep(5); continue
                    return self.create_blocked_analysis("no_json")
                
                result = json.loads(json_text)
                if not all(key in result for key in self.default_analysis.keys()):
                    logger.warning(f"Response JSON missing required keys (attempt {attempt + 1})")
                    if attempt < max_retries - 1: time.sleep(5); continue
                    return self.create_blocked_analysis("invalid_structure")
                return result

            except json.JSONDecodeError as e:
                logger.warning(f"JSON parsing error (attempt {attempt + 1}): {e}")
                if attempt < max_retries - 1: time.sleep(5); continue
                return self.create_blocked_analysis("json_error")

            except Exception as e:
                error_str = str(e).lower()
                if any(term in error_str for term in ['429', 'quota', 'rate limit']):
                    wait_time = min(300, 60 * (2 ** attempt))
                    logger.warning(f"Rate limit hit. Waiting {wait_time}s (attempt {attempt + 1}).")
                    time.sleep(wait_time)
                else:
                    logger.error(f"API error (attempt {attempt + 1}): {e}")
                    if attempt < max_retries - 1: time.sleep(10 * (2 ** attempt)); continue
                    return self.create_blocked_analysis("api_error")
        
        return self.create_blocked_analysis("max_retries_exceeded")

    # <<< CHANGE 1: Rewritten to read JSON Lines (.jsonl) files
    def load_enriched_data(self, file_path: Path) -> Dict[str, Dict]:
        """Load existing enriched data from a JSON Lines file."""
        if not file_path.exists():
            return {}
        
        enriched_data = {}
        with open(file_path, 'r', encoding='utf-8') as f:
            for i, line in enumerate(f):
                line = line.strip()
                if not line:
                    continue
                try:
                    item = json.loads(line)
                    if 'id' in item:
                        enriched_data[item['id']] = item
                    else:
                        logger.warning(f"Skipping malformed line (no 'id') in {file_path.name} at line {i+1}")
                except json.JSONDecodeError:
                    logger.warning(f"Skipping invalid JSON line in {file_path.name} at line {i+1}")
        return enriched_data
    
    def _process_single_item(self, item: Dict, item_type: str, mineral: str) -> Tuple[str, Optional[Dict]]:
        """Processes a single post or comment, designed to be run in a thread."""
        item_id = item['id']
        title, body = ('', '')
        if item_type == "Post":
            title, body = item.get('title', ''), item.get('selftext', '')
        else: # Comment
            body = item.get('body', '')
        
        prompt = self.create_analysis_prompt(mineral=mineral, title=title, body=body)
        analysis = self.analyze_content(prompt)
        
        # <<< CHANGE 2: The result now includes the item_id for direct writing
        if analysis:
            return item_id, {'id': item_id, 'analysis': analysis}
        return item_id, None

    # <<< CHANGE 3: Completely rewritten to use append-only writing (JSON Lines)
    def _process_items(self, item_type: str, items: List[Dict], enriched_items: Dict, 
                       stats: ProcessingStats, mineral: str, mineral_dir: Path):
        """Processes items concurrently and appends results to a JSONL file."""
        # Note: We now use a .jsonl extension to signify JSON Lines format
        output_file = mineral_dir / f"{item_type.lower()}s_enriched.jsonl"
        items_to_process = [item for item in items if item['id'] not in enriched_items]
        
        if not items_to_process:
            logger.info(f"No new {item_type}s to process. All are already enriched.")
            return

        total_to_process = len(items_to_process)
        logger.info(f"Processing {total_to_process} new {item_type}s using {self.num_workers} workers. Results will be appended to {output_file.name}.")
        
        processed_count_in_run = 0
        # Open the file in append mode. This is the core of the change.
        with ThreadPoolExecutor(max_workers=self.num_workers) as executor, \
             open(output_file, 'a', encoding='utf-8') as f_out:
            
            future_to_item_id = {
                executor.submit(self._process_single_item, item, item_type, mineral): item['id'] 
                for item in items_to_process
            }
            
            for future in as_completed(future_to_item_id):
                item_id = future_to_item_id[future]
                processed_count_in_run += 1
                
                try:
                    _item_id, result_data = future.result()
                    
                    if result_data:
                        # Write the single result as a new line in the file
                        f_out.write(json.dumps(result_data, ensure_ascii=False) + '\n')
                        
                        analysis = result_data['analysis']
                        if analysis.get('topic_classification') == 'blocked':
                            stats.increment(blocked=True)
                            logger.info(f"({processed_count_in_run}/{total_to_process}) 🚫 {item_type} {item_id} blocked.")
                        else:
                            stats.increment(successful=True)
                            logger.info(f"({processed_count_in_run}/{total_to_process}) ✅ {item_type} {item_id} successfully analyzed and saved.")
                    else:
                        stats.increment(failed=True)
                        logger.error(f"({processed_count_in_run}/{total_to_process}) ❌ {item_type} {item_id} failed analysis.")

                except Exception as exc:
                    stats.increment(failed=True)
                    logger.error(f"({processed_count_in_run}/{total_to_process}) ❌ {item_type} {item_id} generated an exception: {exc}")

    # <<< CHANGE 4: Update file paths to reflect .json or .jsonl
    def process_mineral(self, mineral: str):
        """Process all data for a single mineral."""
        logger.info(f"--- Starting enrichment for '{mineral}' ---")
        mineral_dir = Path(f"data/{mineral}")
        if not mineral_dir.is_dir():
            logger.error(f"Directory not found for mineral: {mineral}")
            return

        try:
            with open(mineral_dir / "posts.json", 'r', encoding='utf-8') as f:
                all_posts = json.load(f)
            with open(mineral_dir / "comments.json", 'r', encoding='utf-8') as f:
                all_comments = json.load(f)
        except (IOError, json.JSONDecodeError) as e:
            logger.error(f"Could not load source data for {mineral}: {e}")
            return
        
        # The new output files will be .jsonl, but we can gracefully handle old .json files
        enriched_posts_path_jsonl = mineral_dir / "posts_enriched.jsonl"
        enriched_comments_path_jsonl = mineral_dir / "comments_enriched.jsonl"
        enriched_posts_path_json = mineral_dir / "posts_enriched.json"
        enriched_comments_path_json = mineral_dir / "comments_enriched.json"

        # Prefer .jsonl, but fall back to .json for backward compatibility
        enriched_posts = self.load_enriched_data(
            enriched_posts_path_jsonl if enriched_posts_path_jsonl.exists() else enriched_posts_path_json
        )
        enriched_comments = self.load_enriched_data(
            enriched_comments_path_jsonl if enriched_comments_path_jsonl.exists() else enriched_comments_path_json
        )
        
        post_stats = ProcessingStats()
        post_stats.add_skipped(len(enriched_posts))
        comment_stats = ProcessingStats()
        comment_stats.add_skipped(len(enriched_comments))

        logger.info(f"Loaded {len(enriched_posts)} existing enriched posts and {len(enriched_comments)} comments.")
        
        #relevant_posts = [p for p in all_posts if self.is_content_relevant(f"{p.get('title','')} {p.get('selftext','')}", mineral)]
        #relevant_comments = [c for c in all_comments if self.is_content_relevant(c.get('body',''), mineral)]
        #logger.info(f"Filtered to {len(relevant_posts)} relevant posts and {len(relevant_comments)} relevant comments.")
        logger.info(f"Processing ALL {len(all_posts)} posts and {len(all_comments)} comments (no relevance filtering).")
        # Note: The batch_size argument is no longer needed for _process_items
        self._process_items("Post", all_posts, enriched_posts, post_stats, mineral, mineral_dir)
        self._process_items("Comment", all_comments, enriched_comments, comment_stats, mineral, mineral_dir)
        
        self._create_summary(mineral, mineral_dir, post_stats, comment_stats, 
                             len(enriched_posts) + post_stats.successful, 
                             len(enriched_comments) + comment_stats.successful)
        logger.info(f"--- Completed enrichment for '{mineral}' ---")

    def _create_summary(self, mineral: str, mineral_dir: Path, post_stats: ProcessingStats,
                       comment_stats: ProcessingStats, total_posts: int, total_comments: int):
        """Create and save a summary of the enrichment process."""
        summary = {
            'enrichment_date': datetime.now().isoformat(), 'mineral': mineral,
            'model_used': 'gemini-1.5-flash-latest',
            'posts': {
                'total_enriched_in_file': total_posts, 'processed_this_run': post_stats.total_processed,
                'successful': post_stats.successful, 'failed': post_stats.failed,
                'blocked': post_stats.blocked, 'skipped_this_run': post_stats.skipped,
                'success_rate': f"{post_stats.success_rate:.1f}%"
            },
            'comments': {
                'total_enriched_in_file': total_comments, 'processed_this_run': comment_stats.total_processed,
                'successful': comment_stats.successful, 'failed': comment_stats.failed,
                'blocked': comment_stats.blocked, 'skipped_this_run': comment_stats.skipped,
                'success_rate': f"{comment_stats.success_rate:.1f}%"
            }
        }
        
        try:
            with open(mineral_dir / "enrichment_summary.json", "w", encoding="utf-8") as f:
                json.dump(summary, f, indent=2, ensure_ascii=False)
            logger.info(f"Enrichment summary saved for {mineral}.")
        except IOError as e:
            logger.error(f"Failed to save summary for {mineral}: {e}")
    
    def process_all_minerals(self):
        """Process all mineral directories found in the 'data' folder."""
        logger.info("🚀 Starting AI data enrichment process.")
        
        data_dir = Path("data")
        if not data_dir.exists():
            logger.error("Data directory 'data/' not found. Exiting.")
            return
        
        minerals = [d.name for d in data_dir.iterdir() if d.is_dir()]
        if not minerals:
            logger.warning("No mineral subdirectories found in 'data/'.")
            return
        
        logger.info(f"Found {len(minerals)} minerals to process: {', '.join(minerals)}")
        start_time = time.time()
        
        for mineral in minerals:
            try:
                self.process_mineral(mineral)
            except KeyboardInterrupt:
                logger.warning("Process interrupted by user. Shutting down.")
                break
            except Exception as e:
                logger.critical(f"An unhandled error occurred while processing '{mineral}': {e}", exc_info=True)
                continue
        
        duration = time.time() - start_time
        logger.info(f"🎉 All processing finished in {duration/60:.1f} minutes.")

def main():
    """Main entry point for the script."""
    try:
        enricher = RedditMiningEnricher(num_workers=15)
        enricher.process_all_minerals()
    except (FileNotFoundError, ValueError, Exception) as e:
        logger.critical(f"A fatal error occurred during initialization: {e}", exc_info=True)

if __name__ == "__main__":
    main()