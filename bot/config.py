import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Bot configuration
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN not found in environment variables")

# Database configuration
SUPABASE_URL = os.getenv("SUPABASE_URL", "http://127.0.0.1:54321")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")

# User access control
ALLOWED_USERS = os.getenv("ALLOWED_USERS", "").split(",")
ALLOWED_USERS = [int(user_id.strip()) for user_id in ALLOWED_USERS if user_id.strip()]

# File processing limits
MAX_FILE_SIZE_MB = int(os.getenv("MAX_FILE_SIZE_MB", "10"))
MAX_FILE_SIZE = MAX_FILE_SIZE_MB * 1024 * 1024

# Rate limiting configuration
class RateLimitConfig:
    # Progress update settings
    PROGRESS_UPDATE_INTERVAL = float(os.getenv("PROGRESS_UPDATE_INTERVAL", "2.0"))  # seconds
    PROGRESS_UPDATE_BATCH_SIZE = int(os.getenv("PROGRESS_UPDATE_BATCH_SIZE", "10"))  # update every N records

    # Message editing rate limits
    MIN_EDIT_INTERVAL = 1.5  # Minimum seconds between message edits
    RETRY_AFTER_MULTIPLIER = 1.1  # Multiply retry_after by this to be safe

    # Batch processing
    BATCH_SIZE = int(os.getenv("BATCH_SIZE", "50"))  # Process this many records before yielding
    BATCH_DELAY = float(os.getenv("BATCH_DELAY", "0.1"))  # Delay between batches

    # Error handling
    MAX_RETRY_ATTEMPTS = 3
    RETRY_DELAY = 1.0  # Base delay for retries

# Logging configuration
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
LOG_TO_FILE = os.getenv("LOG_TO_FILE", "false").lower() == "true"
LOG_FILE_PATH = os.getenv("LOG_FILE_PATH", "logs/bot.log")

# File paths
LOGOS_DIR = os.getenv("LOGOS_DIR", "logos")
DEFAULT_LOGOS_FILE = os.path.join(LOGOS_DIR, "top_30_universities_mentions.csv")

# Processing configuration
class ProcessingConfig:
    # Skip settings
    SKIP_NOT_RELEVANT = True
    SKIP_DUPLICATES = True
    SKIP_NO_TAGS = True

    # Team synchronization
    SYNC_TEAMS_AFTER_IMPORT = True
    TEAMS_BATCH_SIZE = 1000

    # Logo matching
    ENABLE_LOGO_MATCHING = True
    LOGO_MATCH_FUZZY = True

    # CSV parsing
    CSV_ENCODING = "utf-8"
    CSV_DELIMITER_AUTO_DETECT = True
    CSV_DEFAULT_DELIMITER = ";"

# Message templates
class Messages:
    WELCOME = """
👋 Welcome to Smartjects Processor Bot!

This bot helps you import smartjects from CSV or XLSX files into Supabase with automatic university logo matching.

📝 **How to use:**
1. Send me a CSV or XLSX file with smartjects data
   - For XLSX files: data should be in the 'smartjects' sheet
2. I'll process it and add new smartjects to the database
3. I'll automatically match university logos where possible

Use /help for more information.
"""

    FILE_TOO_LARGE = "❌ File is too large. Maximum size is {max_size}MB."
    FILE_WRONG_TYPE = "❌ Please send a CSV or XLSX file.\nThe file should have a .csv or .xlsx extension.\n\nFor XLSX files, make sure your data is in a sheet named 'smartjects'."
    ACCESS_DENIED = "❌ Access denied. You are not authorized to use this bot."
    PROCESSING_IN_PROGRESS = "⏳ Processing in progress. Please wait for the current operation to complete.\nUse /cancel to stop the current operation."

    RATE_LIMIT_WARNING = """
⚠️ **Processing interrupted due to rate limits**

The bot hit Telegram's rate limits. This usually happens with large files.
Please wait a moment and try again with a smaller file, or process your data in batches.
"""

# Feature flags
FEATURES = {
    "ENABLE_PROGRESS_UPDATES": True,
    "ENABLE_DETAILED_RESULTS": True,
    "ENABLE_TEAM_SYNC": True,
    "ENABLE_LOGO_MATCHING": True,
    "ENABLE_STATS_TRACKING": True,
}

# Export all configurations
__all__ = [
    'BOT_TOKEN',
    'SUPABASE_URL',
    'SUPABASE_KEY',
    'ALLOWED_USERS',
    'MAX_FILE_SIZE',
    'RateLimitConfig',
    'ProcessingConfig',
    'Messages',
    'FEATURES',
    'LOG_LEVEL',
    'LOG_FORMAT',
    'LOG_TO_FILE',
    'LOG_FILE_PATH',
    'LOGOS_DIR',
    'DEFAULT_LOGOS_FILE',
]
