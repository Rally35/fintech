import sys
import os
import logging
import pandas as pd
import numpy as np
from datetime import datetime

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

# Ścieżka do pliku w kontenerze
EXCEL_PATH = '/data/dane_finansowe.xlsx'

def clean_val(val):
    """
    Konwertuje specyficzne wartości Pandas/Numpy (NaN, NaT) oraz puste stringi na Pythonowe None,
    które jest poprawnie rozumiane przez SQL jako NULL.
    """
    if val is None:
        return None
    
    # Sprawdzenie czy to pandas.NaT lub numpy.nan
    if pd.isna(val):
        return None
        
    # Sprawdzenie czy to string "NaT" lub "nan" (częste przy wczytywaniu CSV)
    s_val = str(val).strip()
    if s_val.lower() in ['nat', 'nan', '', 'none']:
        return None
        
    return val

def run_import():
    logger.info(f"Rozpoczynam proces importu z pliku: {EXCEL_PATH}")
    
    if not os.path.exists(EXCEL_PATH):
        logger.error(f"❌ Plik nie istnieje: {EXCEL_PATH}. Sprawdź czy folder 'data' jest poprawnie zamontowany.")
        return

    df = None
    
    # 1. Wczytanie danych (Próba Excel -> Fallback CSV)
    try:
        # Próba 1: Standardowe wczytanie przez ExcelImporter
        df, errors = ExcelImporter.load_excel(EXCEL_PATH)
        
        is_zip_error = errors and any("not a zip file" in str(e) for e in errors)
        
        if is_zip_error:
            logger.warning(f"⚠️ Wykryto problem z formatem pliku (File is not a zip file).")
            logger.info("🔄 Próbuję wczytać plik jako CSV (fallback)...")
            try:
                # Próba 2: Wczytanie jako CSV
                try:
                    df = pd.read_csv(EXCEL_PATH)
                except:
                    df = pd.read_csv(EXCEL_PATH, sep=';')
                
                logger.info(f"✅ Udało się wczytać jako CSV! Znaleziono {len(df)} wierszy.")
                errors = [] 
            except Exception as csv_e:
                logger.error(f"❌ Nie udało się wczytać pliku ani jako Excel, ani jako CSV. Błąd: {csv_e}")
                return

        elif errors:
            for error in errors:
                logger.error(error)
            return

    except Exception as e:
        logger.error(f"Krytyczny błąd podczas odczytu: {e}")
        return

    if df is None or df.empty:
        logger.error("❌ DataFrame jest pusty. Przerywam.")
        return

    logger.info(f"Rozpoczynam przetwarzanie i zapis do DB...")

    # 2. Przygotowanie danych
    try:
        df_clean = ExcelImporter.prepare_for_db(df)
    except Exception as e:
        logger.error(f"Błąd podczas przygotowywania danych (prepare_for_db): {e}")
        return
    
    # 3. Zapis do bazy danych
    try:
        db = DatabaseConnection()
        success_count = 0
        
        with db.get_connection() as conn:
            with conn.cursor() as cur:
                for index, row in df_clean.iterrows():
                    try:
                        query = """
                            INSERT INTO financials (
                                ticker, waluta, data_publikacji, rok, kwartal, 
                                przychody, koszty_sprzedanych_produktow, zysk_brutto_ze_sprzedazy, 
                                koszty_operacyjne, ebitda, amortyzacja, ebit, 
                                przychody_finansowe, koszty_finansowe, zysk_brutto, 
                                podatek_dochodowy, zysk_netto, zysk_netto_jednostki_dominujacej, 
                                aktywa_obrotowe, srodki_pieniezne, naleznosci_krotkoterminowe, 
                                zapasy, pozostale_aktywa_obrotowe, aktywa_trwale, 
                                rzeczowe_aktywa_trwale, wartosci_niematerialne, inwestycje_dlugoterminowe, 
                                pozostale_aktywa_trwale, aktywa_razem, zobowiazania_krotkoterminowe, 
                                dlug_krotkoterminowy, zobowiazania_handlowe, pozostale_zobowiazania_krotkoterminowe, 
                                zobowiazania_dlugoterminowe, dlug_dlugoterminowy, pozostale_zobowiazania_dlugoterminowe, 
                                kapital_wlasny, kapital_zakladowy, kapital_zapasowy, 
                                zyski_zatrzymane, pasywa_razem, przeplywy_operacyjne, 
                                przeplywy_inwestycyjne, przeplywy_finansowe, zmiana_stanu_srodkow, 
                                capex, free_cash_flow, liczba_akcji
                            ) VALUES (
                                %s, %s, %s, %s, %s, 
                                %s, %s, %s, 
                                %s, %s, %s, %s, 
                                %s, %s, %s, 
                                %s, %s, %s, 
                                %s, %s, %s, 
                                %s, %s, %s, 
                                %s, %s, %s, 
                                %s, %s, %s, 
                                %s, %s, %s, 
                                %s, %s, %s, 
                                %s, %s, %s, 
                                %s, %s, %s, 
                                %s, %s, %s, 
                                %s, %s, %s
                            )
                            ON CONFLICT (ticker, rok, kwartal) DO UPDATE 
                            SET 
                                waluta = EXCLUDED.waluta,
                                data_publikacji = EXCLUDED.data_publikacji,
                                przychody = EXCLUDED.przychody,
                                koszty_sprzedanych_produktow = EXCLUDED.koszty_sprzedanych_produktow,
                                zysk_brutto_ze_sprzedazy = EXCLUDED.zysk_brutto_ze_sprzedazy,
                                koszty_operacyjne = EXCLUDED.koszty_operacyjne,
                                ebitda = EXCLUDED.ebitda,
                                amortyzacja = EXCLUDED.amortyzacja,
                                ebit = EXCLUDED.ebit,
                                przychody_finansowe = EXCLUDED.przychody_finansowe,
                                koszty_finansowe = EXCLUDED.koszty_finansowe,
                                zysk_brutto = EXCLUDED.zysk_brutto,
                                podatek_dochodowy = EXCLUDED.podatek_dochodowy,
                                zysk_netto = EXCLUDED.zysk_netto,
                                zysk_netto_jednostki_dominujacej = EXCLUDED.zysk_netto_jednostki_dominujacej,
                                aktywa_obrotowe = EXCLUDED.aktywa_obrotowe,
                                srodki_pieniezne = EXCLUDED.srodki_pieniezne,
                                naleznosci_krotkoterminowe = EXCLUDED.naleznosci_krotkoterminowe,
                                zapasy = EXCLUDED.zapasy,
                                pozostale_aktywa_obrotowe = EXCLUDED.pozostale_aktywa_obrotowe,
                                aktywa_trwale = EXCLUDED.aktywa_trwale,
                                rzeczowe_aktywa_trwale = EXCLUDED.rzeczowe_aktywa_trwale,
                                wartosci_niematerialne = EXCLUDED.wartosci_niematerialne,
                                inwestycje_dlugoterminowe = EXCLUDED.inwestycje_dlugoterminowe,
                                pozostale_aktywa_trwale = EXCLUDED.pozostale_aktywa_trwale,
                                aktywa_razem = EXCLUDED.aktywa_razem,
                                zobowiazania_krotkoterminowe = EXCLUDED.zobowiazania_krotkoterminowe,
                                dlug_krotkoterminowy = EXCLUDED.dlug_krotkoterminowy,
                                zobowiazania_handlowe = EXCLUDED.zobowiazania_handlowe,
                                pozostale_zobowiazania_krotkoterminowe = EXCLUDED.pozostale_zobowiazania_krotkoterminowe,
                                zobowiazania_dlugoterminowe = EXCLUDED.zobowiazania_dlugoterminowe,
                                dlug_dlugoterminowy = EXCLUDED.dlug_dlugoterminowy,
                                pozostale_zobowiazania_dlugoterminowe = EXCLUDED.pozostale_zobowiazania_dlugoterminowe,
                                kapital_wlasny = EXCLUDED.kapital_wlasny,
                                kapital_zakladowy = EXCLUDED.kapital_zakladowy,
                                kapital_zapasowy = EXCLUDED.kapital_zapasowy,
                                zyski_zatrzymane = EXCLUDED.zyski_zatrzymane,
                                pasywa_razem = EXCLUDED.pasywa_razem,
                                przeplywy_operacyjne = EXCLUDED.przeplywy_operacyjne,
                                przeplywy_inwestycyjne = EXCLUDED.przeplywy_inwestycyjne,
                                przeplywy_finansowe = EXCLUDED.przeplywy_finansowe,
                                zmiana_stanu_srodkow = EXCLUDED.zmiana_stanu_srodkow,
                                capex = EXCLUDED.capex,
                                free_cash_flow = EXCLUDED.free_cash_flow,
                                liczba_akcji = EXCLUDED.liczba_akcji,
                                updated_at = NOW();
                        """
                        
                        # Pobieranie wartości z użyciem funkcji czyszczącej clean_val
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
                            clean_val(row.get('Zobowiazania_Dlugoterminowe')), clean_val(row.get('Dlug_Dlugoterminowy')), clean_val(row.get('Pozostale_Zobowiazania_Dlugoterminowe')),
                            clean_val(row.get('Kapital_Wlasny')), clean_val(row.get('Kapital_Zakladowy')), clean_val(row.get('Kapital_Zapasowy')),
                            clean_val(row.get('Zyski_Zatrzymane')), clean_val(row.get('Pasywa_Razem')), clean_val(row.get('Przeplywy_Operacyjne')),
                            clean_val(row.get('Przeplywy_Inwestycyjne')), clean_val(row.get('Przeplywy_Finansowe')), clean_val(row.get('Zmiana_Stanu_Srodkow')),
                            clean_val(row.get('Capex')), clean_val(row.get('Free_Cash_Flow')), clean_val(row.get('Liczba_Akcji'))
                        )

                        cur.execute(query, values)
                        success_count += 1
                        
                    except Exception as e:
                        logger.error(f"Błąd przy importowaniu wiersza dla {row.get('Ticker', 'UNKNOWN')} ({row.get('Rok')} {row.get('Kwartal')}): {e}")
                        conn.rollback()
                        continue
                
                conn.commit()
                
        logger.info(f"✅ Sukces! Zaktualizowano/Dodano {success_count} raportów finansowych.")
        
    except Exception as e:
        logger.error(f"Ogólny błąd połączenia z bazą lub importu: {e}")

if __name__ == "__main__":
    run_import()