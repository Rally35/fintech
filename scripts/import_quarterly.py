import sys
import os
import logging
from datetime import datetime

# ==========================================
# KONFIGURACJA ŚCIEŻEK
# ==========================================
# Dodajemy katalog nadrzędny do sys.path, aby widzieć moduł 'utils'
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from utils.db import DatabaseConnection
from utils.excel_import import ExcelImporter

# Setup logowania
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Ścieżka do pliku w kontenerze (zmapowany wolumen)
EXCEL_PATH = '/data/dane_finansowe.xlsx'

def run_import():
    logger.info(f"Rozpoczynam proces importu z pliku: {EXCEL_PATH}")
    
    if not os.path.exists(EXCEL_PATH):
        logger.error(f"❌ Plik nie istnieje: {EXCEL_PATH}. Upewnij się, że plik jest w folderze data/.")
        return

    # 1. Wczytanie danych z Excela (tylko pierwszy arkusz, gdzie są dane)
    try:
        df, errors = ExcelImporter.load_excel(EXCEL_PATH)
        if errors:
            for error in errors:
                logger.error(error)
            return
    except Exception as e:
        logger.error(f"Krytyczny błąd podczas odczytu Excela: {e}")
        return

    logger.info(f"Znaleziono {len(df)} wierszy. Rozpoczynam przetwarzanie i zapis do DB...")

    # 2. Przygotowanie danych (czyszczenie NaN, formatowanie)
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
                        # Pełne zapytanie INSERT z wszystkimi kolumnami z Excela
                        # Zakładam, że nazwy kolumn w bazie są w snake_case (np. zysk_netto)
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
                        
                        # Pobieranie wartości z wiersza (używamy .get, aby uniknąć błędu przy braku kolumny)
                        values = (
                            row.get('Ticker'), row.get('Waluta'), row.get('Data_publikacji'), row.get('Rok'), row.get('Kwartal'),
                            row.get('Przychody'), row.get('Koszty_Sprzedanych_Produktow'), row.get('Zysk_Brutto_Ze_Sprzedazy'),
                            row.get('Koszty_Operacyjne'), row.get('EBITDA'), row.get('Amortyzacja'), row.get('EBIT'),
                            row.get('Przychody_Finansowe'), row.get('Koszty_Finansowe'), row.get('Zysk_Brutto'),
                            row.get('Podatek_Dochodowy'), row.get('Zysk_Netto'), row.get('Zysk_Netto_Jednostki_Dominujacej'),
                            row.get('Aktywa_Obrotowe'), row.get('Srodki_Pieniezne'), row.get('Naleznosci_Krotkoterminowe'),
                            row.get('Zapasy'), row.get('Pozostale_Aktywa_Obrotowe'), row.get('Aktywa_Trwale'),
                            row.get('Rzeczowe_Aktywa_Trwale'), row.get('Wartosci_Niematerialne'), row.get('Inwestycje_Dlugoterminowe'),
                            row.get('Pozostale_Aktywa_Trwale'), row.get('Aktywa_Razem'), row.get('Zobowiazania_Krotkoterminowe'),
                            row.get('Dlug_Krotkoterminowy'), row.get('Zobowiazania_Handlowe'), row.get('Pozostale_Zobowiazania_Krotkoterminowe'),
                            row.get('Zobowiazania_Dlugoterminowe'), row.get('Dlug_Dlugoterminowy'), row.get('Pozostale_Zobowiazania_Dlugoterminowe'),
                            row.get('Kapital_Wlasny'), row.get('Kapital_Zakladowy'), row.get('Kapital_Zapasowy'),
                            row.get('Zyski_Zatrzymane'), row.get('Pasywa_Razem'), row.get('Przeplywy_Operacyjne'),
                            row.get('Przeplywy_Inwestycyjne'), row.get('Przeplywy_Finansowe'), row.get('Zmiana_Stanu_Srodkow'),
                            row.get('Capex'), row.get('Free_Cash_Flow'), row.get('Liczba_Akcji')
                        )

                        cur.execute(query, values)
                        success_count += 1
                        
                    except Exception as e:
                        logger.error(f"Błąd przy importowaniu wiersza dla {row.get('Ticker', 'UNKNOWN')} ({row.get('Rok')} {row.get('Kwartal')}): {e}")
                        conn.rollback() # Cofnij transakcję dla tego jednego błędu, ale kontynuuj pętlę (lub przerwij, zależnie od strategii)
                        # Tutaj kontynuujemy, aby spróbować wgrać resztę
                        continue
                
                # Zatwierdź wszystkie zmiany na koniec
                conn.commit()
                
        logger.info(f"✅ Sukces! Zaktualizowano/Dodano {success_count} raportów finansowych.")
        
    except Exception as e:
        logger.error(f"Ogólny błąd połączenia z bazą lub importu: {e}")

if __name__ == "__main__":
    run_import()