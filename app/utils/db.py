"""
Database operations and queries
"""

import os
import pandas as pd
from sqlalchemy import create_engine, text
import logging
from datetime import datetime
from contextlib import contextmanager

logger = logging.getLogger(__name__)

class DatabaseConnection:
    """PostgreSQL connection pool and operations"""
    
    def __init__(self):
        self.database_url = os.getenv('DATABASE_URL')
        if not self.database_url:
            raise ValueError("DATABASE_URL not set")
        
        # Initialize SQLAlchemy engine
        try:
            self.engine = create_engine(self.database_url, pool_size=10, max_overflow=20)
        except Exception as e:
            logger.error(f"Failed to create SQLAlchemy engine: {e}")
            raise e
    
    @contextmanager
    def get_connection(self):
        """Context manager for database connections"""
        with self.engine.begin() as conn:
            yield conn
    
    def get_all_companies(self):
        """Get all companies from database"""
        try:
            with self.get_connection() as conn:
                result = conn.execute(text("SELECT ticker, name, currency FROM companies ORDER BY ticker")).mappings().all()
                return result
        except Exception as e:
            logger.error(f"Error fetching companies: {e}")
            return []
    
    def get_latest_price(self, ticker):
        """Get latest price for ticker"""
        try:
            with self.get_connection() as conn:
                query = text("SELECT date, open, high, low, close, volume FROM prices_daily WHERE ticker = :ticker ORDER BY date DESC LIMIT 1")
                result = conn.execute(query, {'ticker': ticker}).mappings().fetchone()
                if result:
                    return result
                return None
        except Exception as e:
            logger.error(f"Error fetching price for {ticker}: {e}")
            return None
    
    def get_latest_financials(self, ticker):
        """Get latest financial report for ticker"""
        try:
            with self.get_connection() as conn:
                query = text("SELECT * FROM financials WHERE ticker = :ticker ORDER BY rok DESC, kwartal DESC LIMIT 1")
                result = conn.execute(query, {'ticker': ticker}).mappings().fetchone()
                if result:
                    return result
                return None
        except Exception as e:
            logger.error(f"Error fetching financials for {ticker}: {e}")
            return None
    
    def get_financials_history(self, ticker, quarters=16):
        """Get financial history for ticker as DataFrame"""
        try:
            query = text("""
                SELECT *,
                       CAST(rok AS VARCHAR) || '-' || kwartal AS period
                FROM financials
                WHERE ticker = :ticker
                ORDER BY rok ASC, kwartal ASC
            """)
            
            with self.get_connection() as conn:
                df = pd.read_sql(query, conn, params={'ticker': ticker})
                
            if df.empty:
                return None
            
            # Sort by year/quarter ensuring correct order
            df = df.sort_values(['rok', 'kwartal'])
            
            # Return last N quarters
            return df.tail(quarters)
        except Exception as e:
            logger.error(f"Error fetching financials history: {e}")
            return None
    
    def get_last_price_update(self):
        """Get timestamp of last price update"""
        try:
            with self.get_connection() as conn:
                result = conn.execute(text("SELECT MAX(date) as last_update FROM prices_daily")).fetchone()
                if result and result[0]:
                    return result[0].strftime("%Y-%m-%d %H:%M")
                return None
        except Exception as e:
            logger.error(f"Error fetching last update: {e}")
            return None
    
    def insert_price(self, ticker, date, open_price, high, low, close, volume):
        """Insert daily price"""
        try:
            with self.get_connection() as conn:
                query = text("""
                    INSERT INTO prices_daily (ticker, date, open, high, low, close, volume)
                    VALUES (:ticker, :date, :open, :high, :low, :close, :volume)
                    ON CONFLICT (ticker, date) DO UPDATE
                    SET open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low,
                        close=EXCLUDED.close, volume=EXCLUDED.volume
                """)
                conn.execute(query, {'ticker': ticker, 'date': date, 'open': open_price, 'high': high, 'low': low, 'close': close, 'volume': volume})
        except Exception as e:
            logger.error(f"Error inserting price: {e}")

    def close(self):
        """Dispose the engine."""
        self.engine.dispose()