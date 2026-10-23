import sys
import os

# Add root to sys.path so we can import app.py
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import app

# Vercel uses the `app` variable to serve the requests
