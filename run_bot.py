#!/usr/bin/env python3
"""
Smartjects Telegram Bot Runner
This script provides a convenient way to start the bot with proper checks
"""

import os
import sys
import subprocess
from pathlib import Path

def check_environment():
    """Check if the environment is properly set up"""
    errors = []

    # Check Python version
    if sys.version_info < (3, 8):
        errors.append(f"Python 3.8+ required, found {sys.version}")

    # Check if .env file exists
    env_file = Path(".env")
    if not env_file.exists():
        errors.append(".env file not found. Copy .env.example to .env and configure it.")
    else:
        # Check for required environment variables
        from dotenv import load_dotenv
        load_dotenv()

        if not os.getenv("BOT_TOKEN"):
            errors.append("BOT_TOKEN not set in .env file")

    # Check if required directories exist
    required_dirs = ["bot", "bot/handlers", "bot/services"]
    for dir_name in required_dirs:
        if not Path(dir_name).exists():
            errors.append(f"Required directory '{dir_name}' not found")

    # Check if main.py exists
    main_file = Path("bot/main.py")
    if not main_file.exists():
        errors.append("bot/main.py not found")

    return errors

def check_dependencies():
    """Check if all required dependencies are installed"""
    try:
        import aiogram
        import supabase
        import dotenv
        import pydantic
        import aiofiles
    except ImportError as e:
        return [f"Missing dependency: {e.name}. Run 'pip install -r requirements.txt'"]
    return []

def main():
    """Main function to run the bot"""
    print("🤖 Smartjects Telegram Bot Startup")
    print("=" * 50)

    # Check environment
    print("Checking environment...")
    env_errors = check_environment()
    if env_errors:
        print("❌ Environment check failed:")
        for error in env_errors:
            print(f"  - {error}")
        sys.exit(1)
    print("✅ Environment check passed")

    # Check dependencies
    print("\nChecking dependencies...")
    dep_errors = check_dependencies()
    if dep_errors:
        print("❌ Dependency check failed:")
        for error in dep_errors:
            print(f"  - {error}")
        sys.exit(1)
    print("✅ Dependencies check passed")

    # Optional: Create logos directory if it doesn't exist
    logos_dir = Path("logos")
    if not logos_dir.exists():
        print("\n📁 Creating logos directory...")
        logos_dir.mkdir()
        print("  - Created 'logos' directory")
        print("  - Place your university logos CSV file here")

    # Start the bot
    print("\n🚀 Starting bot...")
    print("=" * 50)

    try:
        # Run the bot
        subprocess.run([sys.executable, "bot/main.py"], check=True)
    except subprocess.CalledProcessError as e:
        print(f"\n❌ Bot exited with error code: {e.returncode}")
        sys.exit(e.returncode)
    except KeyboardInterrupt:
        print("\n\n👋 Bot stopped by user")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
