"""
Caching utilities
"""

from functools import lru_cache
import logging

logger = logging.getLogger(__name__)

def cached_query(maxsize: int = 128, ttl_seconds: int = 3600):
    """
    Decorator for caching query results. 
    Note: TTL is not natively supported by lru_cache, 
    Streamlit has its own st.cache_data for UI, this is for pure python logic.
    """
    def decorator(func):
        cached_func = lru_cache(maxsize=maxsize)(func)
        return cached_func
    return decorator