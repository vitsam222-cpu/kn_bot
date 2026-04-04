"""Configuration module for the Telegram bot."""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Bot configuration
BOT_TOKEN: str = os.getenv("BOT_TOKEN", "")
if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN is not set in environment variables")

# Database configuration
DATABASE_PATH: str = os.getenv("DATABASE_PATH", "./bot_database.db")

# Logging configuration
LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")

# Base directory
BASE_DIR: Path = Path(__file__).resolve().parent
