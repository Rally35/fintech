"""
Financial metrics calculations
"""

import logging
from typing import Optional

logger = logging.getLogger(__name__)

class MetricsCalculator:
    """Calculate financial metrics from price and financial data"""
    
    def __init__(self, price_data: dict, financial_data: dict):
        """
        Args:
            price_data: {date, open, high, low, close, volume}
            financial_data: {przychody, ebitda, zysk_netto, kapital_wlasny, ...}
        """
        self.price_data = price_data or {}
        self.financial_data = financial_data or {}
    
    def _safe_div(self, num, denom):
        """Helper for safe division"""
        if not num or not denom or denom == 0:
            return None
        return float(num) / float(denom)

    def calculate_pe_ratio(self) -> Optional[float]:
        """Price-to-Earnings Ratio = Price / EPS"""
        try:
            price = self.price_data.get('close')
            liczba_akcji = self.financial_data.get('liczba_akcji')
            zysk_netto = self.financial_data.get('zysk_netto')
            
            if not price or not liczba_akcji or not zysk_netto:
                return None
            
            eps = self._safe_div(zysk_netto, liczba_akcji)
            pe = self._safe_div(price, eps)
            return pe if pe and pe > 0 else None
        except Exception as e:
            logger.error(f"Error calculating P/E: {e}")
            return None
    
    def calculate_roe(self) -> Optional[float]:
        """Return on Equity = Net Income / Shareholder Equity * 100"""
        try:
            zysk_netto = self.financial_data.get('zysk_netto')
            kapital_wlasny = self.financial_data.get('kapital_wlasny')
            
            val = self._safe_div(zysk_netto, kapital_wlasny)
            return val * 100 if val is not None else None
        except Exception as e:
            logger.error(f"Error calculating ROE: {e}")
            return None
    
    def calculate_roa(self) -> Optional[float]:
        """Return on Assets = Net Income / Total Assets * 100"""
        try:
            zysk_netto = self.financial_data.get('zysk_netto')
            aktywa_razem = self.financial_data.get('aktywa_razem')
            
            val = self._safe_div(zysk_netto, aktywa_razem)
            return val * 100 if val is not None else None
        except Exception as e:
            logger.error(f"Error calculating ROA: {e}")
            return None
    
    def calculate_ebitda_margin(self) -> Optional[float]:
        """EBITDA Margin = EBITDA / Revenue * 100"""
        try:
            ebitda = self.financial_data.get('ebitda')
            przychody = self.financial_data.get('przychody')
            
            val = self._safe_div(ebitda, przychody)
            return val * 100 if val is not None else None
        except Exception as e:
            logger.error(f"Error calculating EBITDA margin: {e}")
            return None
    
    def calculate_net_margin(self) -> Optional[float]:
        """Net Margin = Net Income / Revenue * 100"""
        try:
            zysk_netto = self.financial_data.get('zysk_netto')
            przychody = self.financial_data.get('przychody')
            
            val = self._safe_div(zysk_netto, przychody)
            return val * 100 if val is not None else None
        except Exception as e:
            logger.error(f"Error calculating net margin: {e}")
            return None
    
    def calculate_debt_to_equity(self) -> Optional[float]:
        """Debt-to-Equity = Total Debt / Shareholder Equity"""
        try:
            dlug_krotkoterminowy = self.financial_data.get('dlug_krotkoterminowy') or 0
            dlug_dlugoterminowy = self.financial_data.get('dlug_dlugoterminowy') or 0
            kapital_wlasny = self.financial_data.get('kapital_wlasny')
            
            total_debt = dlug_krotkoterminowy + dlug_dlugoterminowy
            return self._safe_div(total_debt, kapital_wlasny)
        except Exception as e:
            logger.error(f"Error calculating D/E: {e}")
            return None
    
    def calculate_current_ratio(self) -> Optional[float]:
        """Current Ratio = Current Assets / Current Liabilities"""
        try:
            aktywa_obrotowe = self.financial_data.get('aktywa_obrotowe')
            zobowiazania_krotkoterminowe = self.financial_data.get('zobowiazania_krotkoterminowe')
            
            return self._safe_div(aktywa_obrotowe, zobowiazania_krotkoterminowe)
        except Exception as e:
            logger.error(f"Error calculating current ratio: {e}")
            return None