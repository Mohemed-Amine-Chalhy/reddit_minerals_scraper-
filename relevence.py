import json
import os
import google.generativeai as genai
from datetime import datetime
import time
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MineralRelevanceChecker:
    def __init__(self, api_key):
        """Initialize the Gemini API client"""
        genai.configure(api_key=api_key)
        self.model = genai.GenerativeModel('gemini-1.5-flash')
        
    def analyze_post_relevance(self, mineral, post_data, sample_comments=None):
        """
        Analyze if a post is actually about the specified mineral
        """
        # Prepare the prompt
        prompt = f"""
        You are analyzing whether a Reddit post is genuinely about the mineral/material "{mineral}" in a business, investment, mining, or industrial context.

        POST TITLE: {post_data.get('title', '')}
        POST CONTENT: {post_data.get('selftext', '')[:1000]}  # Limit content length
        SUBREDDIT: {post_data.get('subreddit', '')}
        
        """
        
        # Add sample comments if provided
        if sample_comments:
            prompt += "\nSAMPLE COMMENTS:\n"
            for i, comment in enumerate(sample_comments[:3]):  # Limit to 3 comments
                prompt += f"Comment {i+1}: {comment.get('body', '')[:300]}\n"
        
        prompt += f"""
        
        Task: Determine if this post is GENUINELY about {mineral} as a mineral/material/commodity.

        Consider relevant contexts:
        - Mining, extraction, or production of {mineral}
        - Industrial uses and applications of {mineral}
        - Market prices, trading, or investment in {mineral}
        - Companies involved in {mineral} business
        - Supply chain or geopolitical issues affecting {mineral}
        - Technical properties or specifications of {mineral}

        Ignore irrelevant contexts:
        - Unrelated topics that just happen to mention the word
        - Metaphorical or casual uses of the term
        - Different meanings of the word (e.g., "gold" meaning good, "silver" as color)
        - Gaming, entertainment, or fictional contexts unless specifically about the actual mineral

        Respond with ONLY:
        RELEVANT: [confidence 0-100] - [brief reason]
        OR
        NOT_RELEVANT: [confidence 0-100] - [brief reason]
        
        Example responses:
        RELEVANT: 85 - Post discusses lithium mining operations and market demand
        NOT_RELEVANT: 95 - Post uses "gold" metaphorically to mean valuable, not about actual gold
        """
        
        try:
            response = self.model.generate_content(prompt)
            return self.parse_response(response.text)
        except Exception as e:
            logger.error(f"Error analyzing post {post_data.get('id', 'unknown')}: {e}")
            return None
    
    def parse_response(self, response_text):
        """Parse the Gemini response into structured data"""
        try:
            response_text = response_text.strip()
            
            if response_text.startswith("RELEVANT:"):
                parts = response_text[9:].strip().split(" - ", 1)
                confidence = int(parts[0])
                reason = parts[1] if len(parts) > 1 else ""
                return {
                    'relevant': True,
                    'confidence': confidence,
                    'reason': reason
                }
            elif response_text.startswith("NOT_RELEVANT:"):
                parts = response_text[13:].strip().split(" - ", 1)
                confidence = int(parts[0])
                reason = parts[1] if len(parts) > 1 else ""
                return {
                    'relevant': False,
                    'confidence': confidence,
                    'reason': reason
                }
            else:
                logger.warning(f"Unexpected response format: {response_text}")
                return None
                
        except Exception as e:
            logger.error(f"Error parsing response: {e}")
            return None

class ProgressTracker:
    """Enhanced progress tracking with better resume functionality"""
    
    def __init__(self, mineral_dir):
        self.mineral_dir = mineral_dir
        self.analysis_file = f"{mineral_dir}/relevance_analysis.json"
        self.progress_file = f"{mineral_dir}/analysis_progress.json"
        
    def load_existing_analysis(self):
        """Load existing analysis results"""
        if os.path.exists(self.analysis_file):
            try:
                with open(self.analysis_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    logger.info(f"Loaded {len(data)} existing analysis results")
                    return data
            except Exception as e:
                logger.error(f"Error loading existing analysis: {e}")
                return {}
        return {}
    
    def load_progress(self):
        """Load progress information"""
        if os.path.exists(self.progress_file):
            try:
                with open(self.progress_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                logger.error(f"Error loading progress: {e}")
                return {}
        return {}
    
    def save_analysis_results(self, analysis_results):
        """Save analysis results with backup"""
        try:
            # Create backup of existing file
            if os.path.exists(self.analysis_file):
                backup_file = f"{self.analysis_file}.backup"
                import shutil
                shutil.copy2(self.analysis_file, backup_file)
            
            # Save new results
            with open(self.analysis_file, 'w', encoding='utf-8') as f:
                json.dump(analysis_results, f, indent=2, ensure_ascii=False)
            
            logger.debug(f"Saved {len(analysis_results)} analysis results")
            
        except Exception as e:
            logger.error(f"Error saving analysis results: {e}")
            raise
    
    def save_progress(self, progress_info):
        """Save progress information"""
        try:
            progress_info['last_updated'] = datetime.now().isoformat()
            with open(self.progress_file, 'w', encoding='utf-8') as f:
                json.dump(progress_info, f, indent=2, ensure_ascii=False)
            
            logger.debug("Progress saved")
            
        except Exception as e:
            logger.error(f"Error saving progress: {e}")

def get_sample_comments(comments_data, post_id, max_comments=3):
    """Get sample comments for a post"""
    post_comments = [c for c in comments_data if c.get('post_id') == post_id]
    # Sort by score to get most relevant comments
    post_comments.sort(key=lambda x: x.get('score', 0), reverse=True)
    return post_comments[:max_comments]

def analyze_mineral_folder(mineral_name, mineral_dir, checker, confidence_threshold=70):
    """Analyze all posts in a mineral folder with enhanced resume capability"""
    
    # Initialize progress tracker
    tracker = ProgressTracker(mineral_dir)
    
    # Load existing data
    posts_file = f"{mineral_dir}/posts.json"
    comments_file = f"{mineral_dir}/comments.json"
    
    if not os.path.exists(posts_file):
        logger.error(f"Posts file not found: {posts_file}")
        return
    
    with open(posts_file, 'r', encoding='utf-8') as f:
        posts_data = json.load(f)
    
    comments_data = []
    if os.path.exists(comments_file):
        with open(comments_file, 'r', encoding='utf-8') as f:
            comments_data = json.load(f)
    
    # Load existing analysis and progress
    analysis_results = tracker.load_existing_analysis()
    progress_info = tracker.load_progress()
    
    # Determine which posts still need analysis
    total_posts = len(posts_data)
    analyzed_post_ids = set(analysis_results.keys())
    remaining_posts = [post for post in posts_data if post['id'] not in analyzed_post_ids]
    
    logger.info(f"Analysis status for {mineral_name}:")
    logger.info(f"  Total posts: {total_posts}")
    logger.info(f"  Already analyzed: {len(analyzed_post_ids)}")
    logger.info(f"  Remaining to analyze: {len(remaining_posts)}")
    
    if not remaining_posts:
        logger.info(f"✅ All posts already analyzed for {mineral_name}")
        create_summary(mineral_name, mineral_dir, analysis_results, confidence_threshold)
        return
    
    # Continue analysis from where we left off
    current_count = len(analyzed_post_ids)
    
    for i, post in enumerate(remaining_posts):
        post_id = post['id']
        
        logger.info(f"Analyzing post {current_count + i + 1}/{total_posts}: {post['title'][:100]}...")
        
        # Get sample comments for context
        sample_comments = get_sample_comments(comments_data, post_id)
        
        # Analyze the post
        result = checker.analyze_post_relevance(mineral_name, post, sample_comments)
        
        if result:
            analysis_results[post_id] = {
                'relevant': result['relevant'],
                'confidence': result['confidence'],
                'reason': result['reason'],
                'analyzed_at': datetime.now().isoformat(),
                'title': post['title'],
                'subreddit': post['subreddit']
            }
            
            if result['relevant'] and result['confidence'] >= confidence_threshold:
                logger.info(f"✅ RELEVANT ({result['confidence']}%): {result['reason']}")
            else:
                logger.info(f"❌ NOT RELEVANT ({result['confidence']}%): {result['reason']}")
        else:
            logger.warning(f"Failed to analyze post {post_id}")
            continue
        
        # Save progress every 5 analyses (more frequent saves)
        if (current_count + i + 1) % 5 == 0:
            tracker.save_analysis_results(analysis_results)
            
            progress_info.update({
                'total_posts': total_posts,
                'analyzed_posts': len(analysis_results),
                'last_analyzed_post_id': post_id,
                'last_position': current_count + i + 1
            })
            tracker.save_progress(progress_info)
            
            logger.info(f"🔄 Progress saved: {len(analysis_results)}/{total_posts} analyzed")
        
        # Rate limiting
        time.sleep(1)
    
    # Final save
    tracker.save_analysis_results(analysis_results)
    
    final_progress = {
        'total_posts': total_posts,
        'analyzed_posts': len(analysis_results),
        'completed': True,
        'completion_date': datetime.now().isoformat()
    }
    tracker.save_progress(final_progress)
    
    # Create summary
    create_summary(mineral_name, mineral_dir, analysis_results, confidence_threshold)
    
    logger.info(f"✅ Analysis complete for {mineral_name}")

def create_summary(mineral_name, mineral_dir, analysis_results, confidence_threshold):
    """Create summary of analysis results"""
    total_analyzed = len(analysis_results)
    relevant_posts = [r for r in analysis_results.values() if r['relevant']]
    high_confidence_relevant = [r for r in relevant_posts if r['confidence'] >= confidence_threshold]
    
    # Group by subreddit
    subreddit_stats = {}
    for result in analysis_results.values():
        subreddit = result.get('subreddit', 'unknown')
        if subreddit not in subreddit_stats:
            subreddit_stats[subreddit] = {'total': 0, 'relevant': 0}
        subreddit_stats[subreddit]['total'] += 1
        if result['relevant'] and result['confidence'] >= confidence_threshold:
            subreddit_stats[subreddit]['relevant'] += 1
    
    summary = {
        'mineral': mineral_name,
        'analysis_date': datetime.now().isoformat(),
        'total_analyzed': total_analyzed,
        'total_relevant': len(relevant_posts),
        'high_confidence_relevant': len(high_confidence_relevant),
        'confidence_threshold': confidence_threshold,
        'relevance_rate': len(high_confidence_relevant) / total_analyzed if total_analyzed > 0 else 0,
        'subreddit_breakdown': subreddit_stats,
        'confidence_distribution': {
            '90-100': len([r for r in analysis_results.values() if r['confidence'] >= 90]),
            '80-89': len([r for r in analysis_results.values() if 80 <= r['confidence'] < 90]),
            '70-79': len([r for r in analysis_results.values() if 70 <= r['confidence'] < 80]),
            '60-69': len([r for r in analysis_results.values() if 60 <= r['confidence'] < 70]),
            'below-60': len([r for r in analysis_results.values() if r['confidence'] < 60])
        }
    }
    
    with open(f"{mineral_dir}/relevance_summary.json", 'w', encoding='utf-8') as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)
    
    logger.info(f"Summary for {mineral_name}:")
    logger.info(f"  Total analyzed: {total_analyzed}")
    logger.info(f"  High-confidence relevant: {len(high_confidence_relevant)}")
    logger.info(f"  Relevance rate: {summary['relevance_rate']:.2%}")

def show_overall_progress():
    """Show progress across all minerals"""
    data_dir = "data"
    if not os.path.exists(data_dir):
        logger.error("Data directory not found")
        return
    
    mineral_dirs = [d for d in os.listdir(data_dir) 
                   if os.path.isdir(os.path.join(data_dir, d))]
    
    logger.info("\n📊 Overall Progress Summary:")
    logger.info("=" * 50)
    
    for mineral_name in sorted(mineral_dirs):
        mineral_dir = os.path.join(data_dir, mineral_name)
        posts_file = f"{mineral_dir}/posts.json"
        analysis_file = f"{mineral_dir}/relevance_analysis.json"
        
        if not os.path.exists(posts_file):
            continue
        
        # Count total posts
        with open(posts_file, 'r', encoding='utf-8') as f:
            total_posts = len(json.load(f))
        
        # Count analyzed posts
        analyzed_posts = 0
        if os.path.exists(analysis_file):
            with open(analysis_file, 'r', encoding='utf-8') as f:
                analyzed_posts = len(json.load(f))
        
        progress_pct = (analyzed_posts / total_posts * 100) if total_posts > 0 else 0
        status = "✅ COMPLETE" if analyzed_posts == total_posts else "🔄 IN PROGRESS"
        
        logger.info(f"{mineral_name:15} | {analyzed_posts:4}/{total_posts:4} ({progress_pct:5.1f}%) | {status}")

def main():
    # Configure your Gemini API key
    API_KEY = "AIzaSyBpiTYBj0kDFD3ujPzppOINNSGhiJAA6xs"  # Replace with your actual API key
    
    if API_KEY == "your-gemini-api-key-here":
        logger.error("Please set your actual Gemini API key in the script")
        return
    
    # Show current progress
    show_overall_progress()
    
    # Ask user if they want to continue
    response = input("\nDo you want to continue/start analysis? (y/n): ").lower().strip()
    if response not in ['y', 'yes']:
        logger.info("Analysis cancelled by user")
        return
    
    # Initialize the checker
    checker = MineralRelevanceChecker(API_KEY)
    
    # Get list of mineral directories
    data_dir = "data"
    mineral_dirs = [d for d in os.listdir(data_dir) 
                   if os.path.isdir(os.path.join(data_dir, d))]
    
    # Analyze each mineral
    for mineral_name in sorted(mineral_dirs):
        mineral_dir = os.path.join(data_dir, mineral_name)
        
        try:
            analyze_mineral_folder(mineral_name, mineral_dir, checker)
        except KeyboardInterrupt:
            logger.info(f"\n⏸️  Analysis interrupted by user. Progress has been saved.")
            logger.info("You can resume by running the script again.")
            break
        except Exception as e:
            logger.error(f"Error analyzing {mineral_name}: {e}")
            continue
    
    logger.info("\n🎉 Analysis session complete!")
    show_overall_progress()

if __name__ == "__main__":
    main()