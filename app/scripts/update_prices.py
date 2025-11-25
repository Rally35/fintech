#!/usr/bin/env python3
"""
Background job: Update daily stock prices
"""

import os
import sys
import logging
from datetime import datetime, timedelta
import psycopg2
from psycopg2.extras import RealDictCursor
import yfinance as yf

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv('DATABASE_URL')

def get_db_connection():
    """Connect to database"""
    if not DATABASE_URL:
        logger.error("DATABASE_URL environment variable not set")
        sys.exit(1)
    try:
        conn = psycopg2.connect(DATABASE_URL)
        return conn
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        raise

def get_tickers():
    """Get all tickers from database"""
    try:
        conn = get_db_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT ticker FROM companies ORDER BY ticker")
            tickers = [row['ticker'] for row in cur.fetchall()]
        conn.close()
        return tickers
    except Exception as e:
        logger.error(f"Error fetching tickers: {e}")
        return []

def update_price(ticker: str):
    """Update price for single ticker"""
    try:
        logger.info(f"Updating price for {ticker}...")

        # Fetch from yfinance - PROSTSZE WYWOŁANIE
        data = yf.download(
            ticker,
            start=datetime.now() - timedelta(days=5),
            end=datetime.now(),
            progress=False
            # USUNIĘTO: session=session <--- Pozwalamy YF działać samemu
        )
        
        if data.empty:
            logger.warning(f"No data found for {ticker}")
            return False
        
        # Get latest row
        latest = data.iloc[-1]
        date = data.index[-1].strftime('%Y-%m-%d')
        
        # Handle yfinance multi-index columns if present, or scalar values
        try:
            close_val = float(latest['Close'])
            open_val = float(latest['Open'])
            high_val = float(latest['High'])
            low_val = float(latest['Low'])
            vol_val = int(latest['Volume'])
        except Exception:
             # Fallback for some yfinance versions returning scalars directly
            close_val = float(latest['Close'].iloc[0]) if hasattr(latest['Close'], 'iloc') else float(latest['Close'])
            open_val = float(latest['Open'].iloc[0]) if hasattr(latest['Open'], 'iloc') else float(latest['Open'])
            high_val = float(latest['High'].iloc[0]) if hasattr(latest['High'], 'iloc') else float(latest['High'])
            low_val = float(latest['Low'].iloc[0]) if hasattr(latest['Low'], 'iloc') else float(latest['Low'])
            vol_val = int(latest['Volume'].iloc[0]) if hasattr(latest['Volume'], 'iloc') else int(latest['Volume'])

        
        # Insert into database
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO prices_daily 
                (ticker, date, open, high, low, close, volume)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (ticker, date) DO UPDATE
                SET open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low,
                    close=EXCLUDED.close, volume=EXCLUDED.volume
            """, (
                ticker,
                date,
                open_val,
                high_val,
                low_val,
                close_val,
                vol_val
            ))
        conn.commit()
        conn.close()
        
        logger.info(f"✅ Updated {ticker}: {close_val:.2f}")
        return True
    
    except Exception as e:
        logger.error(f"Error updating {ticker}: {e}")
        return False

def main():
    """Main job execution"""
    logger.info("Starting price update job...")
    
    tickers = get_tickers()
    if not tickers:
        logger.error("No tickers found! Make sure to import companies first.")
        sys.exit(0)
    
    logger.info(f"Found {len(tickers)} tickers to update")
    
    success_count = 0
    error_count = 0
    
    for ticker in tickers:
        if update_price(ticker):
            success_count += 1
        else:
            error_count += 1
    
    logger.info(f"Job completed: {success_count} success, {error_count} errors")
    
    if error_count > 0:
        # Don't exit with 1 to avoid crashing systemd service, just log error
        pass

if __name__ == '__main__':
    main()