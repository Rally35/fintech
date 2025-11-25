-- Tabela spółek
CREATE TABLE IF NOT EXISTS companies (
    ticker VARCHAR(10) PRIMARY KEY,
    name VARCHAR(255),
    currency VARCHAR(3) DEFAULT 'PLN',
    sector VARCHAR(100),
    industry VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tabela danych finansowych (48 kolumn z Excela)
CREATE TABLE IF NOT EXISTS financials (
    id SERIAL PRIMARY KEY,
    ticker VARCHAR(10) NOT NULL, -- Opcjonalnie: REFERENCES companies(ticker) jeśli chcesz ścisłej relacji
    waluta VARCHAR(10),
    data_publikacji DATE,
    rok INT,
    kwartal VARCHAR(10),
    
    -- Rachunek Zysków i Strat (RZiS)
    przychody NUMERIC,
    koszty_sprzedanych_produktow NUMERIC,
    zysk_brutto_ze_sprzedazy NUMERIC,
    koszty_operacyjne NUMERIC,
    ebitda NUMERIC,
    amortyzacja NUMERIC,
    ebit NUMERIC,
    przychody_finansowe NUMERIC,
    koszty_finansowe NUMERIC,
    zysk_brutto NUMERIC,
    podatek_dochodowy NUMERIC,
    zysk_netto NUMERIC,
    zysk_netto_jednostki_dominujacej NUMERIC,
    
    -- Bilans - Aktywa
    aktywa_obrotowe NUMERIC,
    srodki_pieniezne NUMERIC,
    naleznosci_krotkoterminowe NUMERIC,
    zapasy NUMERIC,
    pozostale_aktywa_obrotowe NUMERIC,
    aktywa_trwale NUMERIC,
    rzeczowe_aktywa_trwale NUMERIC,
    wartosci_niematerialne NUMERIC,
    inwestycje_dlugoterminowe NUMERIC,
    pozostale_aktywa_trwale NUMERIC,
    aktywa_razem NUMERIC,
    
    -- Bilans - Pasywa
    zobowiazania_krotkoterminowe NUMERIC,
    dlug_krotkoterminowy NUMERIC,
    zobowiazania_handlowe NUMERIC,
    pozostale_zobowiazania_krotkoterminowe NUMERIC,
    zobowiazania_dlugoterminowe NUMERIC,
    dlug_dlugoterminowy NUMERIC,
    pozostale_zobowiazania_dlugoterminowe NUMERIC,
    kapital_wlasny NUMERIC,
    kapital_zakladowy NUMERIC,
    kapital_zapasowy NUMERIC,
    zyski_zatrzymane NUMERIC,
    pasywa_razem NUMERIC,
    
    -- Cash Flow
    przeplywy_operacyjne NUMERIC,
    przeplywy_inwestycyjne NUMERIC,
    przeplywy_finansowe NUMERIC,
    zmiana_stanu_srodkow NUMERIC,
    capex NUMERIC,
    free_cash_flow NUMERIC,
    
    -- Inne
    liczba_akcji BIGINT,
    
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Klucz unikalny, aby ON CONFLICT działało w imporcie
    CONSTRAINT unique_financial_report UNIQUE (ticker, rok, kwartal)
);

-- Daily prices
CREATE TABLE IF NOT EXISTS prices_daily (
    id SERIAL PRIMARY KEY,
    ticker VARCHAR(10) NOT NULL REFERENCES companies(ticker) ON DELETE CASCADE,
    date DATE NOT NULL,
    open DECIMAL(10,2),
    high DECIMAL(10,2),
    low DECIMAL(10,2),
    close DECIMAL(10,2) NOT NULL,
    volume BIGINT,
    created_at TIMESTAMP DEFAULT NOW(),
    
    UNIQUE(ticker, date)
);

-- Indeksy dla szybszego wyszukiwania
CREATE INDEX IF NOT EXISTS idx_financials_ticker ON financials(ticker);
CREATE INDEX IF NOT EXISTS idx_financials_ticker ON financials(rok, kwartal);
CREATE INDEX IF NOT EXISTS idx_prices_ticker_date ON prices_daily(ticker, date DESC);
CREATE INDEX IF NOT EXISTS idx_prices_date ON prices_daily(date DESC);