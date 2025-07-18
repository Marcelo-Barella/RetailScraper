"""
Configuration settings for the retailScraper project.
Centralizes paths and settings used across the application.
"""
import os
from dotenv import load_dotenv
load_dotenv(override=True)

# Base temp directory - change this to use D: drive instead of C:
TEMP_BASE_DIR = os.getenv("TEMP_BASE_DIR")

# Specific temp directories
TEMP_BROWSER_SESSIONS_DIR = os.path.join(TEMP_BASE_DIR, "browser_sessions")
TEMP_BROWSER_SESSIONS_POOL_DIR = os.path.join(TEMP_BASE_DIR, "browser_sessions_pool")

# Ensure the base temp directory exists
os.makedirs(TEMP_BASE_DIR, exist_ok=True) 