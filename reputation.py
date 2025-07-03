import json
import os
import google.generativeai as genai
from datetime import datetime
import time
import logging
import statistics
from collections import defaultdict

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ReputationAnalyzer:
    def __init__(self, api_key):
        """Initialize the Gemini API client for reputation analysis"""
        genai.configure(api_key=api_key)
        self.model = genai.GenerativeModel('gemini-1.5-flash')
        
    def analyze_post_reputation(self, mineral, post_data, comments_data=None):
        """
        Analyze the reputation and public perception of a post about a mineral
        """
        # Prepare the prompt
        prompt = f"""
        You are analyzing the reputation and public perception of a Reddit post about the mineral/material "{mineral}".

        POST TITLE: {post_data.get('title', '')}
        POST CONTENT: {post_data.get('selftext', '')[:1500]}
        SUBREDDIT: {post_data.get('subreddit', '')}
        SCORE: {post_data.get('score', 0)}
        UPVOTE RATIO: {post_data.get('upvote_ratio', 0)}
        NUM COMMENTS: {post_data.get('num_comments', 0)}
        """
        
        # Add comments for sentiment analysis
        if comments_data:
            prompt += "\nTOP COMMENTS:\n"
            for i, comment in enumerate(comments_data[:5]):  # Analyze top 5 comments
                prompt += f"Comment {i+1} (Score: {comment.get('score', 0)}): {comment.get('body', '')[:400]}\n"
        
        prompt += f"""
        
        Task: Analyze the reputation and public perception of this post about {mineral}.

        Please evaluate the following aspects:

        1. SENTIMENT: Overall emotional tone (positive, negative, neutral)
        2. CREDIBILITY: How trustworthy/reliable the information appears
        3. EXPERTISE_LEVEL: Level of technical knowledge demonstrated
        4. CONTROVERSY: Whether the topic is controversial or disputed
        5. MARKET_IMPACT: Potential impact on market perception of {mineral}
        6. PUBLIC_INTEREST: Level of public engagement and interest
        7. INFORMATION_QUALITY: Quality and accuracy of information presented

        Respond with ONLY a JSON object in this exact format:
        {{
            "sentiment": "positive/negative/neutral",
            "sentiment_score": <number between -100 to 100>,
            "credibility": "high/medium/low",
            "credibility_score": <number between 0 to 100>,
            "expertise_level": "expert/intermediate/novice",
            "expertise_score": <number between 0 to 100>,
            "controversy_level": "high/medium/low/none",
            "controversy_score": <number between 0 to 100>,
            "market_impact": "very_positive/positive/neutral/negative/very_negative",
            "market_impact_score": <number between -100 to 100>,
            "public_interest": "very_high/high/medium/low/very_low",
            "public_interest_score": <number between 0 to 100>,
            "information_quality": "excellent/good/fair/poor",
            "information_quality_score": <number between 0 to 100>,
            "overall_reputation_score": <number between -100 to 100>,
            "key_themes": ["theme1", "theme2", "theme3"],
            "reputation_summary": "brief summary of reputation implications"
        }}

        Guidelines:
        - sentiment_score: -100 (very negative) to +100 (very positive)
        - credibility_score: 0 (not credible) to 100 (highly credible)
        - expertise_score: 0 (no expertise) to 100 (expert level)
        - controversy_score: 0 (no controversy) to 100 (highly controversial)
        - market_impact_score: -100 (very negative impact) to +100 (very positive impact)
        - public_interest_score: 0 (no interest) to 100 (very high interest)
        - information_quality_score: 0 (poor quality) to 100 (excellent quality)
        - overall_reputation_score: weighted average considering all factors
        """
        
        try:
            response = self.model.generate_content(prompt)
            return self.parse_reputation_response(response.text)
        except Exception as e:
            logger.error(f"Error analyzing reputation for post {post_data.get('id', 'unknown')}: {e}")
            return None
    
    def parse_reputation_response(self, response_text):
        """Parse the Gemini response into structured reputation data"""
        try:
            # Clean up the response to extract JSON
            response_text = response_text.strip()
            
            # Find JSON object in response
            start_idx = response_text.find('{')
            end_idx = response_text.rfind('}') + 1
            
            if start_idx == -1 or end_idx == 0:
                logger.warning("No JSON object found in response")
                return None
            
            json_str = response_text[start_idx:end_idx]
            result = json.loads(json_str)
            
            # Add timestamp
            result['analyzed_at'] = datetime.now().isoformat()
            
            return result
                
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing JSON response: {e}")
            logger.error(f"Response text: {response_text}")
            return None
        except Exception as e:
            logger.error(f"Error parsing reputation response: {e}")
            return None

class ReputationProgressTracker:
    """Progress tracking for reputation analysis"""
    
    def __init__(self, mineral_dir):
        self.mineral_dir = mineral_dir
        self.reputation_file = f"{mineral_dir}/reputation_analysis.json"
        self.reputation_progress_file = f"{mineral_dir}/reputation_progress.json"
        
    def load_existing_reputation_analysis(self):
        """Load existing reputation analysis results"""
        if os.path.exists(self.reputation_file):
            try:
                with open(self.reputation_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    logger.info(f"Loaded {len(data)} existing reputation analysis results")
                    return data
            except Exception as e:
                logger.error(f"Error loading existing reputation analysis: {e}")
                return {}
        return {}
    
    def save_reputation_results(self, reputation_results):
        """Save reputation analysis results with backup"""
        try:
            # Create backup of existing file
            if os.path.exists(self.reputation_file):
                backup_file = f"{self.reputation_file}.backup"
                import shutil
                shutil.copy2(self.reputation_file, backup_file)
            
            # Save new results
            with open(self.reputation_file, 'w', encoding='utf-8') as f:
                json.dump(reputation_results, f, indent=2, ensure_ascii=False)
            
            logger.debug(f"Saved {len(reputation_results)} reputation analysis results")
            
        except Exception as e:
            logger.error(f"Error saving reputation results: {e}")
            raise
    
    def save_progress(self, progress_info):
        """Save progress information"""
        try:
            progress_info['last_updated'] = datetime.now().isoformat()
            with open(self.reputation_progress_file, 'w', encoding='utf-8') as f:
                json.dump(progress_info, f, indent=2, ensure_ascii=False)
            
            logger.debug("Reputation progress saved")
            
        except Exception as e:
            logger.error(f"Error saving reputation progress: {e}")

def get_post_comments(comments_data, post_id, min_score=1, max_comments=10):
    """Get relevant comments for a post, filtered by score"""
    post_comments = [c for c in comments_data if c.get('post_id') == post_id and c.get('score', 0) >= min_score]
    # Sort by score to get most relevant comments
    post_comments.sort(key=lambda x: x.get('score', 0), reverse=True)
    return post_comments[:max_comments]

def analyze_mineral_reputation(mineral_name, mineral_dir, analyzer):
    """Analyze reputation for all relevant posts in a mineral folder"""
    
    # Initialize progress tracker
    tracker = ReputationProgressTracker(mineral_dir)
    
    # Load required data files
    posts_file = f"{mineral_dir}/posts.json"
    comments_file = f"{mineral_dir}/comments.json"
    relevance_file = f"{mineral_dir}/relevance_analysis.json"
    
    if not os.path.exists(posts_file):
        logger.error(f"Posts file not found: {posts_file}")
        return
    
    if not os.path.exists(relevance_file):
        logger.error(f"Relevance analysis file not found: {relevance_file}")
        logger.info("Please run the relevance analysis first")
        return
    
    # Load data
    with open(posts_file, 'r', encoding='utf-8') as f:
        posts_data = json.load(f)
    
    with open(relevance_file, 'r', encoding='utf-8') as f:
        relevance_data = json.load(f)
    
    comments_data = []
    if os.path.exists(comments_file):
        with open(comments_file, 'r', encoding='utf-8') as f:
            comments_data = json.load(f)
    
    # Filter for relevant posts only (high confidence)
    relevant_post_ids = [
        post_id for post_id, analysis in relevance_data.items()
        if analysis.get('relevant', False) ]
    
    relevant_posts = [post for post in posts_data if post['id'] in relevant_post_ids]
    
    logger.info(f"Reputation analysis for {mineral_name}:")
    logger.info(f"  Total posts: {len(posts_data)}")
    logger.info(f"  Relevant posts: {len(relevant_posts)}")
    
    if not relevant_posts:
        logger.info(f"No relevant posts found for {mineral_name}")
        return
    
    # Load existing reputation analysis
    reputation_results = tracker.load_existing_reputation_analysis()
    
    # Determine which posts still need reputation analysis
    analyzed_post_ids = set(reputation_results.keys())
    remaining_posts = [post for post in relevant_posts if post['id'] not in analyzed_post_ids]
    
    logger.info(f"  Already analyzed: {len(analyzed_post_ids)}")
    logger.info(f"  Remaining to analyze: {len(remaining_posts)}")
    
    if not remaining_posts:
        logger.info(f"✅ All relevant posts already analyzed for reputation - {mineral_name}")
        create_reputation_summary(mineral_name, mineral_dir, reputation_results, posts_data, relevance_data)
        return
    
    # Continue analysis from where we left off
    current_count = len(analyzed_post_ids)
    
    for i, post in enumerate(remaining_posts):
        post_id = post['id']
        
        logger.info(f"Analyzing reputation {current_count + i + 1}/{len(relevant_posts)}: {post['title'][:80]}...")
        
        # Get comments for this post
        post_comments = get_post_comments(comments_data, post_id)
        
        # Analyze reputation
        result = analyzer.analyze_post_reputation(mineral_name, post, post_comments)
        
        if result:
            # Add post metadata
            result.update({
                'post_id': post_id,
                'title': post['title'],
                'subreddit': post['subreddit'],
                'post_score': post.get('score', 0),
                'upvote_ratio': post.get('upvote_ratio', 0),
                'num_comments': post.get('num_comments', 0),
                'num_comments_analyzed': len(post_comments)
            })
            
            reputation_results[post_id] = result
            
            logger.info(f"✅ Reputation: {result.get('overall_reputation_score', 0)}/100 | "
                       f"Sentiment: {result.get('sentiment', 'unknown')} | "
                       f"Credibility: {result.get('credibility', 'unknown')}")
        else:
            logger.warning(f"Failed to analyze reputation for post {post_id}")
            continue
        
        # Save progress every 3 analyses
        if (current_count + i + 1) % 3 == 0:
            tracker.save_reputation_results(reputation_results)
            logger.info(f"🔄 Progress saved: {len(reputation_results)}/{len(relevant_posts)} analyzed")
        
        # Rate limiting
        time.sleep(2)  # Slightly longer delay for reputation analysis
    
    # Final save
    tracker.save_reputation_results(reputation_results)
    
    # Create summary
    create_reputation_summary(mineral_name, mineral_dir, reputation_results, posts_data, relevance_data)
    
    logger.info(f"✅ Reputation analysis complete for {mineral_name}")

def create_reputation_summary(mineral_name, mineral_dir, reputation_results, posts_data, relevance_data):
    """Create comprehensive reputation summary"""
    
    if not reputation_results:
        logger.warning(f"No reputation results to summarize for {mineral_name}")
        return
    
    # Calculate statistics
    reputation_scores = [r.get('overall_reputation_score', 0) for r in reputation_results.values()]
    sentiment_scores = [r.get('sentiment_score', 0) for r in reputation_results.values()]
    credibility_scores = [r.get('credibility_score', 0) for r in reputation_results.values()]
    market_impact_scores = [r.get('market_impact_score', 0) for r in reputation_results.values()]
    
    # Count categories
    sentiment_counts = defaultdict(int)
    credibility_counts = defaultdict(int)
    market_impact_counts = defaultdict(int)
    controversy_counts = defaultdict(int)
    
    for result in reputation_results.values():
        sentiment_counts[result.get('sentiment', 'unknown')] += 1
        credibility_counts[result.get('credibility', 'unknown')] += 1
        market_impact_counts[result.get('market_impact', 'unknown')] += 1
        controversy_counts[result.get('controversy_level', 'unknown')] += 1
    
    # Subreddit breakdown
    subreddit_reputation = defaultdict(list)
    for result in reputation_results.values():
        subreddit = result.get('subreddit', 'unknown')
        subreddit_reputation[subreddit].append(result.get('overall_reputation_score', 0))
    
    subreddit_stats = {}
    for subreddit, scores in subreddit_reputation.items():
        subreddit_stats[subreddit] = {
            'post_count': len(scores),
            'avg_reputation': statistics.mean(scores),
            'median_reputation': statistics.median(scores),
            'min_reputation': min(scores),
            'max_reputation': max(scores)
        }
    
    # Top and bottom posts
    sorted_posts = sorted(reputation_results.items(), 
                         key=lambda x: x[1].get('overall_reputation_score', 0), 
                         reverse=True)
    
    top_posts = []
    bottom_posts = []
    
    for post_id, result in sorted_posts[:5]:  # Top 5
        top_posts.append({
            'post_id': post_id,
            'title': result.get('title', ''),
            'subreddit': result.get('subreddit', ''),
            'reputation_score': result.get('overall_reputation_score', 0),
            'sentiment': result.get('sentiment', ''),
            'credibility': result.get('credibility', '')
        })
    
    for post_id, result in sorted_posts[-5:]:  # Bottom 5
        bottom_posts.append({
            'post_id': post_id,
            'title': result.get('title', ''),
            'subreddit': result.get('subreddit', ''),
            'reputation_score': result.get('overall_reputation_score', 0),
            'sentiment': result.get('sentiment', ''),
            'credibility': result.get('credibility', '')
        })
    
    # Create comprehensive summary
    summary = {
        'mineral': mineral_name,
        'analysis_date': datetime.now().isoformat(),
        'total_posts_analyzed': len(reputation_results),
        
        # Overall reputation metrics
        'overall_reputation': {
            'average_score': statistics.mean(reputation_scores),
            'median_score': statistics.median(reputation_scores),
            'min_score': min(reputation_scores),
            'max_score': max(reputation_scores),
            'std_deviation': statistics.stdev(reputation_scores) if len(reputation_scores) > 1 else 0
        },
        
        # Sentiment analysis
        'sentiment_analysis': {
            'distribution': dict(sentiment_counts),
            'average_sentiment_score': statistics.mean(sentiment_scores),
            'median_sentiment_score': statistics.median(sentiment_scores)
        },
        
        # Credibility analysis
        'credibility_analysis': {
            'distribution': dict(credibility_counts),
            'average_credibility_score': statistics.mean(credibility_scores),
            'median_credibility_score': statistics.median(credibility_scores)
        },
        
        # Market impact analysis
        'market_impact_analysis': {
            'distribution': dict(market_impact_counts),
            'average_market_impact_score': statistics.mean(market_impact_scores),
            'median_market_impact_score': statistics.median(market_impact_scores)
        },
        
        # Controversy analysis
        'controversy_analysis': {
            'distribution': dict(controversy_counts)
        },
        
        # Subreddit breakdown
        'subreddit_reputation': subreddit_stats,
        
        # Top and bottom performing posts
        'top_reputation_posts': top_posts,
        'bottom_reputation_posts': bottom_posts,
        
        # Reputation score distribution
        'reputation_score_distribution': {
            'excellent_80_100': len([s for s in reputation_scores if s >= 80]),
            'good_60_79': len([s for s in reputation_scores if 60 <= s < 80]),
            'fair_40_59': len([s for s in reputation_scores if 40 <= s < 60]),
            'poor_20_39': len([s for s in reputation_scores if 20 <= s < 40]),
            'very_poor_below_20': len([s for s in reputation_scores if s < 20])
        }
    }
    
    # Save summary
    with open(f"{mineral_dir}/reputation_summary.json", 'w', encoding='utf-8') as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)
    
    # Log summary statistics
    logger.info(f"Reputation Summary for {mineral_name}:")
    logger.info(f"  Average reputation score: {summary['overall_reputation']['average_score']:.1f}/100")
    logger.info(f"  Most common sentiment: {max(sentiment_counts.items(), key=lambda x: x[1])[0]}")
    logger.info(f"  Most common credibility: {max(credibility_counts.items(), key=lambda x: x[1])[0]}")
    logger.info(f"  Posts with excellent reputation (80+): {summary['reputation_score_distribution']['excellent_80_100']}")

def show_reputation_progress():
    """Show reputation analysis progress across all minerals"""
    data_dir = "data"
    if not os.path.exists(data_dir):
        logger.error("Data directory not found")
        return
    
    mineral_dirs = [d for d in os.listdir(data_dir) 
                   if os.path.isdir(os.path.join(data_dir, d))]
    
    logger.info("\n📊 Reputation Analysis Progress:")
    logger.info("=" * 60)
    
    for mineral_name in sorted(mineral_dirs):
        mineral_dir = os.path.join(data_dir, mineral_name)
        relevance_file = f"{mineral_dir}/relevance_analysis.json"
        reputation_file = f"{mineral_dir}/reputation_analysis.json"
        
        if not os.path.exists(relevance_file):
            logger.info(f"{mineral_name:15} | No relevance analysis found")
            continue
        
        # Count relevant posts
        with open(relevance_file, 'r', encoding='utf-8') as f:
            relevance_data = json.load(f)
        
        relevant_posts = len([
            post_id for post_id, analysis in relevance_data.items()
            if analysis.get('relevant', False) and analysis.get('confidence', 0) >= 70
        ])
        
        # Count reputation analyzed posts
        reputation_analyzed = 0
        if os.path.exists(reputation_file):
            with open(reputation_file, 'r', encoding='utf-8') as f:
                reputation_analyzed = len(json.load(f))
        
        if relevant_posts == 0:
            status = "❌ NO RELEVANT POSTS"
        elif reputation_analyzed == relevant_posts:
            status = "✅ COMPLETE"
        else:
            status = "🔄 IN PROGRESS"
        
        progress_pct = (reputation_analyzed / relevant_posts * 100) if relevant_posts > 0 else 0
        
        logger.info(f"{mineral_name:15} | {reputation_analyzed:3}/{relevant_posts:3} ({progress_pct:5.1f}%) | {status}")

def main():
    # Configure your Gemini API key
    API_KEY = "AIzaSyBpiTYBj0kDFD3ujPzppOINNSGhiJAA6xs"  # Replace with your actual API key
    
    if API_KEY == "your-gemini-api-key-here":
        logger.error("Please set your actual Gemini API key in the script")
        return
    
    # Show current progress
    show_reputation_progress()
    
    # Ask user if they want to continue
    response = input("\nDo you want to continue/start reputation analysis? (y/n): ").lower().strip()
    if response not in ['y', 'yes']:
        logger.info("Reputation analysis cancelled by user")
        return
    
    # Initialize the analyzer
    analyzer = ReputationAnalyzer(API_KEY)
    
    # Get list of mineral directories
    data_dir = "data"
    mineral_dirs = [d for d in os.listdir(data_dir) 
                   if os.path.isdir(os.path.join(data_dir, d))]
    
    # Analyze each mineral
    for mineral_name in sorted(mineral_dirs):
        mineral_dir = os.path.join(data_dir, mineral_name)
        
        try:
            analyze_mineral_reputation(mineral_name, mineral_dir, analyzer)
        except KeyboardInterrupt:
            logger.info(f"\n⏸️  Reputation analysis interrupted by user. Progress has been saved.")
            logger.info("You can resume by running the script again.")
            break
        except Exception as e:
            logger.error(f"Error analyzing reputation for {mineral_name}: {e}")
            continue
    
    logger.info("\n🎉 Reputation analysis session complete!")
    show_reputation_progress()

if __name__ == "__main__":
    main()