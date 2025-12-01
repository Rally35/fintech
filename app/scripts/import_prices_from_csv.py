#!/usr/bin/env python3
"""
This script imports stock prices from a CSV file.
"""

import os
import sys
import logging
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor

# ==========================================
# KONFIGURACJA ŚCIEŻEK
# ==========================================
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from utils.db import DatabaseConnection

# Setup logowania
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Ścieżka do pliku w kontenerze
CSV_PATH = '/app/data/stock_prices.csv'
DATABASE_URL = os.getenv('DATABASE_URL')

def run_price_import():
    """Imports stock prices from the CSV file."""
    logger.info(f"Starting price import from: {CSV_PATH}")
    
    if not os.path.exists(CSV_PATH):
        logger.error(f"❌ File not found: {CSV_PATH}. Check if 'data' folder is correctly mounted.")
        return

    df = None
    try:
        df = pd.read_csv(CSV_PATH)
    except Exception as e:
        logger.error(f"Critical error during file read: {e}")
        return

    if df is None or df.empty:
        logger.error("❌ DataFrame is empty. Aborting.")
        return

    logger.info("Processing and saving to DB...")
    
    try:
        db = DatabaseConnection()
        success_count = 0
        with db.get_connection() as conn:
            with conn.cursor() as cur:
                for index, row in df.iterrows():
                    try:
                        query = """
                            INSERT INTO prices_daily (ticker, date, close)
                            VALUES (%s, %s, %s)
                            ON CONFLICT (ticker, date) DO UPDATE SET 
                                close = EXCLUDED.close,
                                updated_at = NOW();
                        """
                        values = (row.get('ticker'), row.get('date'), row.get('close'))
                        cur.execute(query, values)
                        success_count += 1
                    except Exception as e:
                        logger.error(f"Error importing row for {row.get('ticker')}: {e}")
                        conn.rollback()
                        continue
                conn.commit()
        logger.info(f"✅ Success! Updated/Added {success_count} prices.")
    except Exception as e:
        logger.error(f"General error with database connection or import: {e}")

if __name__ == "__main__":
    run_price_import()
