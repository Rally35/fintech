"""
Excel data import and validation
"""

import pandas as pd
import logging
from typing import Tuple, List

logger = logging.getLogger(__name__)

class ExcelImporter:
    """Import financial data from Excel"""
    
    REQUIRED_COLUMNS = [
        'Ticker', 'Waluta', 'Rok', 'Kwartal', 'Przychody', 
        'Zysk_Netto', 'EBITDA', 'Kapital_Wlasny', 'Aktywa_Razem',
        'Liczba_Akcji'
    ]
    
    @staticmethod
    def load_excel(filepath: str) -> Tuple[pd.DataFrame, List[str]]:
        """
        Load Excel file and validate schema
        Returns: (DataFrame, list of errors)
        """
        errors = []
        
        try:
            df = pd.read_excel(filepath)
            
            # Check columns
            missing_cols = [col for col in ExcelImporter.REQUIRED_COLUMNS 
                           if col not in df.columns]
            if missing_cols:
                errors.append(f"Missing columns: {missing_cols}")
            
            # Check for empty dataframe
            if len(df) == 0:
                errors.append("Excel file is empty")
            
            return (df, errors) if not errors else (None, errors)
        
        except Exception as e:
            logger.error(f"Error loading Excel: {e}")
            errors.append(f"Error reading file: {str(e)}")
            return None, errors
    
    @staticmethod
    def validate_data(df: pd.DataFrame) -> List[str]:
        """Validate data types and values"""
        errors = []
        
        # Check numeric columns
        numeric_cols = ['Przychody', 'Zysk_Netto', 'EBITDA', 'Liczba_Akcji']
        for col in numeric_cols:
            if col in df.columns:
                try:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                    null_count = df[col].isna().sum()
                    if null_count > len(df) * 0.1:  # >10% nulls
                        errors.append(f"Column {col}: {null_count} null values")
                except Exception as e:
                    errors.append(f"Error converting {col} to numeric: {e}")
        
        return errors
    
    @staticmethod
    def prepare_for_db(df: pd.DataFrame) -> pd.DataFrame:
        """Prepare dataframe for database insertion"""
        df_clean = df.copy()
        
        # Fill nulls with 0 for numeric columns
        numeric_cols = df_clean.select_dtypes(include=['number']).columns
        df_clean[numeric_cols] = df_clean[numeric_cols].fillna(0)
        
        # Lowercase columns for DB compatibility if needed, or mapping logic here
        
        return df_clean