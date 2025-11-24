import sys
import os
import logging

# ==========================================
# KONFIGURACJA ŚCIEŻEK
# ==========================================
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from utils.db import DatabaseConnection
from utils.excel_import import ExcelImporter

# Setup logowania
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

EXCEL_PATH = '/data/dane_finansowe.xlsx'

def run_import():
    logger.info(f"Wczytywanie pliku: {EXCEL_PATH}")
    
    if not os.path.exists(EXCEL_PATH):
        logger.error(f"❌ Plik nie istnieje: {EXCEL_PATH}")
        return

    df, errors = ExcelImporter.load_excel(EXCEL_PATH)
    
    if errors:
        for error in errors:
            logger.error(error)
        return

    logger.info(f"Znaleziono {len(df)} wierszy. Przetwarzanie...")

    df_clean = ExcelImporter.prepare_for_db(df)
    
    try:
        db = DatabaseConnection()
        
        success_count = 0
        with db.get_connection() as conn:
            with conn.cursor() as cur:
                for _, row in df_clean.iterrows():
                    try:
                        # ---------------------------------------------------------
                        # KROK 1: Upewnij się, że firma istnieje (Automatyczna Rejestracja)
                        # ---------------------------------------------------------
                        cur.execute("""
                            INSERT INTO companies (ticker, name, currency)
                            VALUES (%s, %s, %s)
                            ON CONFLICT (ticker) DO NOTHING
                        """, (
                            row['Ticker'], 
                            row['Ticker'],  # Używamy tickera jako nazwy tymczasowo
                            row.get('Waluta', 'PLN')
                        ))

                        # ---------------------------------------------------------
                        # KROK 2: Wstaw dane finansowe
                        # ---------------------------------------------------------
                        cur.execute("""
                            INSERT INTO financials_quarterly (
                                ticker, rok, kwartal, przychody, ebitda, zysk_netto, 
                                aktywa_razem, kapital_wlasny, liczba_akcji
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (ticker, rok, kwartal) DO UPDATE 
                            SET przychody = EXCLUDED.przychody,
                                zysk_netto = EXCLUDED.zysk_netto,
                                ebitda = EXCLUDED.ebitda,
                                aktywa_razem = EXCLUDED.aktywa_razem,
                                kapital_wlasny = EXCLUDED.kapital_wlasny,
                                liczba_akcji = EXCLUDED.liczba_akcji
                        """, (
                            row['Ticker'], row['Rok'], row['Kwartal'], 
                            row['Przychody'], row['EBITDA'], row['Zysk_Netto'],
                            row['Aktywa_Razem'], row['Kapital_Wlasny'], row['Liczba_Akcji']
                        ))
                        success_count += 1
                    except Exception as e:
                        logger.error(f"Błąd przy {row['Ticker']}: {e}")
                        conn.rollback()
                conn.commit()
                
        logger.info(f"✅ Sukces! Zaimportowano {success_count} raportów finansowych (firmy zostały dodane automatycznie).")
        
    except Exception as e:
        logger.error(f"Błąd połączenia z bazą: {e}")

if __name__ == "__main__":
    run_import()