#!/usr/bin/env python3
"""
This script updates all financial data, including quarterly reports and daily stock prices.
"""

import os
import sys
import logging
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import psycopg2
from psycopg2.extras import RealDictCursor
import yfinance as yf

# ==========================================
# KONFIGURACJA ŚCIEŻEK
# ==========================================
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from utils.db import DatabaseConnection
from utils.excel_import import ExcelImporter
from import_prices_from_csv import run_price_import

# Setup logowania
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Ścieżka do pliku w kontenerze
EXCEL_PATH = '/app/data/dane_finansowe.xlsx'
DATABASE_URL = os.getenv('DATABASE_URL')

def clean_val(val):
    """
    Konwertuje specyficzne wartości Pandas/Numpy (NaN, NaT) oraz puste stringi na Pythonowe None,
    które jest poprawnie rozumiane przez SQL jako NULL.
    """
    if val is None:
        return None
    if pd.isna(val):
        return None
    s_val = str(val).strip()
    if s_val.lower() in ['nat', 'nan', '', 'none']:
        return None
    return val

def run_quarterly_import():
    """Imports quarterly financial data from the Excel file."""
    logger.info(f"Starting quarterly data import from: {EXCEL_PATH}")
    
    if not os.path.exists(EXCEL_PATH):
        logger.error(f"❌ File not found: {EXCEL_PATH}. Check if 'data' folder is correctly mounted.")
        return

    df = None
    try:
        df, errors = ExcelImporter.load_excel(EXCEL_PATH)
        if errors:
            for error in errors:
                logger.error(error)
            return
    except Exception as e:
        logger.error(f"Critical error during file read: {e}")
        return

    if df is None or df.empty:
        logger.error("❌ DataFrame is empty. Aborting.")
        return

    logger.info("Processing and saving to DB...")
    try:
        df_clean = ExcelImporter.prepare_for_db(df)
    except Exception as e:
        logger.error(f"Error preparing data (prepare_for_db): {e}")
        return
    
    try:
        db = DatabaseConnection()
        success_count = 0
        with db.get_connection() as conn:
            with conn.cursor() as cur:
                for index, row in df_clean.iterrows():
                    try:
                        query = """
                            INSERT INTO financials (
                                ticker, waluta, data_publikacji, rok, kwartal, przychody, koszty_sprzedanych_produktow, zysk_brutto_ze_sprzedazy, 
                                koszty_operacyjne, ebitda, amortyzacja, ebit, przychody_finansowe, koszty_finansowe, zysk_brutto, 
                                podatek_dochodowy, zysk_netto, zysk_netto_jednostki_dominujacej, aktywa_obrotowe, srodki_pieniezne, naleznosci_krotkoterminowe, 
                                zapasy, pozostale_aktywa_obrotowe, aktywa_trwale, rzeczowe_aktywa_trwale, wartosci_niematerialne, inwestycje_dlugoterminowe, 
                                pozostale_aktywa_trwale, aktywa_razem, zobowiazania_krotkoterminowe, dlug_krotkoterminowy, zobowiazania_handlowe, pozostale_zobowiazania_krotkoterminowe, 
                                zobowiazania_dlugoterminowe, dlug_dlugoterminowy, pozostale_zobowiazania_dlugoterminowe, kapital_wlasny, kapital_zakladowy, kapital_zapasowy, 
                                zyski_zatrzymane, pasywa_razem, przeplywy_operacyjne, przeplywy_inwestycyjne, przeplywy_finansowe, zmiana_stanu_srodkow, 
                                capex, free_cash_flow, liczba_akcji
                            ) VALUES (
                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                            )
                            ON CONFLICT (ticker, rok, kwartal) DO UPDATE SET 
                                waluta = EXCLUDED.waluta, data_publikacji = EXCLUDED.data_publikacji, przychody = EXCLUDED.przychody, koszty_sprzedanych_produktow = EXCLUDED.koszty_sprzedanych_produktow,
                                zysk_brutto_ze_sprzedazy = EXCLUDED.zysk_brutto_ze_sprzedazy, koszty_operacyjne = EXCLUDED.koszty_operacyjne, ebitda = EXCLUDED.ebitda, amortyzacja = EXCLUDED.amortyzacja,
                                ebit = EXCLUDED.ebit, przychody_finansowe = EXCLUDED.przychody_finansowe, koszty_finansowe = EXCLUDED.koszty_finansowe, zysk_brutto = EXCLUDED.zysk_brutto,
                                podatek_dochodowy = EXCLUDED.podatek_dochodowy, zysk_netto = EXCLUDED.zysk_netto, zysk_netto_jednostki_dominujacej = EXCLUDED.zysk_netto_jednostki_dominujacej,
                                aktywa_obrotowe = EXCLUDED.aktywa_obrotowe, srodki_pieniezne = EXCLUDED.srodki_pieniezne, naleznosci_krotkoterminowe = EXCLUDED.naleznosci_krotkoterminowe,
                                zapasy = EXCLUDED.zapasy, pozostale_aktywa_obrotowe = EXCLUDED.pozostale_aktywa_obrotowe, aktywa_trwale = EXCLUDED.aktywa_trwale,
                                rzeczowe_aktywa_trwale = EXCLUDED.rzeczowe_aktywa_trwale, wartosci_niematerialne = EXCLUDED.wartosci_niematerialne, inwestycje_dlugoterminowe = EXCLUDED.inwestycje_dlugoterminowe,
                                pozostale_aktywa_trwale = EXCLUDED.pozostale_aktywa_trwale, aktywa_razem = EXCLUDED.aktywa_razem, zobowiazania_krotkoterminowe = EXCLUDED.zobowiazania_krotkoterminowe,
                                dlug_krotkoterminowy = EXCLUDED.dlug_krotkoterminowy, zobowiazania_handlowe = EXCLUDED.zobowiazania_handlowe, pozostale_zobowiazania_krotkoterminowe = EXCLUDED.pozostale_zobowiazania_krotkoterminowe,
                                zobowiazania_dlugoterminowe = EXCLUDED.zobowiazania_dlugoterminowe, dlug_dlugoterminowy = EXCLUDED.dlug_dlugoterminowy, pozostale_zobowiazania_dlugoterminowe = EXCLUDED.pozostale_zobowiazania_dlugoterminowe,
                                kapital_wlasny = EXCLUDED.kapital_wlasny, kapital_zakladowy = EXCLUDED.kapital_zakladowy, kapital_zapasowy = EXCLUDED.kapital_zapasowy,
                                zyski_zatrzymane = EXCLUDED.zyski_zatrzymane, pasywa_razem = EXCLUDED.pasywa_razem, przeplywy_operacyjne = EXCLUDED.przeplywy_operacyjne,
                                przeplywy_inwestycyjne = EXCLUDED.przeplywy_inwestycyjne, przeplywy_finansowe = EXCLUDED.przeplywy_finansowe, zmiana_stanu_srodkow = EXCLUDED.zmiana_stanu_srodkow,
                                capex = EXCLUDED.capex, free_cash_flow = EXCLUDED.free_cash_flow, liczba_akcji = EXCLUDED.liczba_akcji,
                                updated_at = NOW();
                        """
                        values = (
                            clean_val(row.get('Ticker')), clean_val(row.get('Waluta')), clean_val(row.get('Data_publikacji')), clean_val(row.get('Rok')), clean_val(row.get('Kwartal')),
                            clean_val(row.get('Przychody')), clean_val(row.get('Koszty_Sprzedanych_Produktow')), clean_val(row.get('Zysk_Brutto_Ze_Sprzedazy')),
                            clean_val(row.get('Koszty_Operacyjne')), clean_val(row.get('EBITDA')), clean_val(row.get('Amortyzacja')), clean_val(row.get('EBIT')),
                            clean_val(row.get('Przychody_Finansowe')), clean_val(row.get('Koszty_Finansowe')), clean_val(row.get('Zysk_Brutto')),
                            clean_val(row.get('Podatek_Dochodowy')), clean_val(row.get('Zysk_Netto')), clean_val(row.get('Zysk_Netto_Jednostki_Dominujacej')),
                            clean_val(row.get('Aktywa_Obrotowe')), clean_val(row.get('Srodki_Pieniezne')), clean_val(row.get('Naleznosci_Krotkoterminowe')),
                            clean_val(row.get('Zapasy')), clean_val(row.get('Pozostale_Aktywa_Obrotowe')), clean_val(row.get('Aktywa_Trwale')),
                            clean_val(row.get('Rzeczowe_Aktywa_Trwale')), clean_val(row.get('Wartosci_Niematerialne')), clean_val(row.get('Inwestycje_Dlugoterminowe')),
                            clean_val(row.get('Pozostale_Aktywa_Trwale')), clean_val(row.get('Aktywa_Razem')), clean_val(row.get('Zobowiazania_Krotkoterminowe')),
                            clean_val(row.get('Dlug_Krotkoterminowy')), clean_val(row.get('Zobowiazania_Handlowe')), clean_val(row.get('Pozostale_Zobowiazania_Krotkoterminowe')),
                            clean_val(row.get('Zobowiazania_Dlugoterminowe')), clean__val(row.get('Dlug_Dlugoterminowy')), clean_val(row.get('Pozostale_Zobowiazania_Dlugoterminowe')),
                            clean_val(row.get('Kapital_Wlasny')), clean_val(row.get('Kapital_Zakladowy')), clean_val(row.get('Kapital_Zapasowy')),
                            clean_val(row.get('Zyski_Zatrzymane')), clean_val(row.get('Pasywa_Razem')), clean_val(row.get('Przeplywy_Operacyjne')),
                            clean_val(row.get('Przeplywy_Inwestycyjne')), clean_val(row.get('Przeplywy_Finansowe')), clean_val(row.get('Zmiana_Stanu_Srodkow')),
                            clean_val(row.get('Capex')), clean_val(row.get('Free_Cash_Flow')), clean_val(row.get('Liczba_Akcji'))
                        )
                        cur.execute(query, values)
                        success_count += 1
                    except Exception as e:
                        logger.error(f"Error importing row for {row.get('Ticker', 'UNKNOWN')} ({row.get('Rok')} {row.get('Kwartal')}): {e}")
                        conn.rollback()
                        continue
                conn.commit()
        logger.info(f"✅ Success! Updated/Added {success_count} financial reports.")
    except Exception as e:
        logger.error(f"General error with database connection or import: {e}")


def get_db_connection():
    """Establishes a database connection."""
    if not DATABASE_URL:
        logger.error("DATABASE_URL environment variable not set")
        sys.exit(1)
    try:
        return psycopg2.connect(DATABASE_URL)
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        raise

def get_tickers():
    """Fetches all tickers from the database."""
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
    """Updates the price for a single ticker."""
    try:
        logger.info(f"Updating price for {ticker}...")
        data = yf.download(ticker, start=datetime.now() - timedelta(days=5), end=datetime.now(), progress=False, auto_adjust=True)
        if data.empty:
            logger.warning(f"No data found for {ticker}")
            return False
        
        latest = data.iloc[-1]
        date = data.index[-1].strftime('%Y-%m-%d')
        
        close_val = float(latest['Close'])
        open_val = float(latest['Open'])
        high_val = float(latest['High'])
        low_val = float(latest['Low'])
        vol_val = int(latest['Volume'])
        
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO prices_daily (ticker, date, open, high, low, close, volume)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (ticker, date) DO UPDATE
                SET open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low, close=EXCLUDED.close, volume=EXCLUDED.volume
            """, (ticker, date, open_val, high_val, low_val, close_val, vol_val))
        conn.commit()
        conn.close()
        
        logger.info(f"✅ Updated {ticker}: {close_val:.2f}")
        return True
    except Exception as e:
        logger.error(f"Error updating {ticker}: {e}")
        return False

def update_all_prices():
    """Main job for updating all stock prices."""
    logger.info("Starting price update job...")
    tickers = get_tickers()
    if not tickers:
        logger.error("No tickers found! Make sure to import companies first.")
        return
    
    logger.info(f"Found {len(tickers)} tickers to update")
    success_count = sum(1 for ticker in tickers if update_price(ticker))
    error_count = len(tickers) - success_count
    
    logger.info(f"Price update job completed: {success_count} success, {error_count} errors")

if __name__ == "__main__":
    logger.info("--- Starting Full Data Update ---")
    run_quarterly_import()
    run_price_import()
    update_all_prices()
    logger.info("--- Full Data Update Finished ---")
