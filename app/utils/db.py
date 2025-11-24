"""
Database operations and queries
"""

import os
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.pool import SimpleConnectionPool
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
        
        # Initialize pool
        try:
            self.pool = SimpleConnectionPool(
                1, 10,
                self.database_url,
                connect_timeout=5
            )
        except Exception as e:
            logger.error(f"Failed to create connection pool: {e}")
            raise e
    
    @contextmanager
    def get_connection(self):
        """Context manager for database connections"""
        conn = self.pool.getconn()
        try:
            yield conn
        finally:
            self.pool.putconn(conn)
    
    def get_all_companies(self):
        """Get all companies from database"""
        try:
            with self.get_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute("""
                        SELECT id, ticker, name, currency 
                        FROM companies 
                        ORDER BY ticker
                    """)
                    return cur.fetchall()
        except Exception as e:
            logger.error(f"Error fetching companies: {e}")
            return []
    
    def get_latest_price(self, ticker):
        """Get latest price for ticker"""
        try:
            with self.get_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute("""
                        SELECT date, open, high, low, close, volume
                        FROM prices_daily
                        WHERE ticker = %s
                        ORDER BY date DESC
                        LIMIT 1
                    """, (ticker,))
                    result = cur.fetchone()
                    if result:
                        return dict(result)
                    return None
        except Exception as e:
            logger.error(f"Error fetching price for {ticker}: {e}")
            return None
    
    def get_latest_financials(self, ticker):
        """Get latest financial report for ticker"""
        try:
            with self.get_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute("""
                        SELECT *
                        FROM financials_quarterly
                        WHERE ticker = %s
                        ORDER BY rok DESC, kwartal DESC
                        LIMIT 1
                    """, (ticker,))
                    result = cur.fetchone()
                    if result:
                        return dict(result)
                    return None
        except Exception as e:
            logger.error(f"Error fetching financials for {ticker}: {e}")
            return None
    
    def get_financials_history(self, ticker, quarters=16):
        """Get financial history for ticker as DataFrame"""
        try:
            import pandas as pd
            
            with self.get_connection() as conn:
                query = """
                    SELECT *,
                           CAST(rok AS VARCHAR) || '-' || kwartal AS period
                    FROM financials_quarterly
                    WHERE ticker = %s
                    ORDER BY rok ASC, kwartal ASC
                """
                
                # Fetching all and limiting in pandas for easier sort/manipulation
                df = pd.read_sql(query, conn, params=(ticker,))
                
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
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute("SELECT MAX(date) as last_update FROM prices_daily")
                    result = cur.fetchone()
                    if result and result['last_update']:
                        return result['last_update'].strftime("%Y-%m-%d %H:%M")
                    return None
        except Exception as e:
            logger.error(f"Error fetching last update: {e}")
            return None
    
    def insert_price(self, ticker, date, open_price, high, low, close, volume):
        """Insert daily price"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO prices_daily (ticker, date, open, high, low, close, volume)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (ticker, date) DO UPDATE
                        SET open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low,
                            close=EXCLUDED.close, volume=EXCLUDED.volume
                    """, (ticker, date, open_price, high, low, close, volume))
                conn.commit()
        except Exception as e:
            logger.error(f"Error inserting price: {e}")
    
    def close(self):
        """Close all connections in pool"""
        if self.pool:
            self.pool.closeall()