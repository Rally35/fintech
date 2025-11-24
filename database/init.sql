-- ============================================================
-- SCHEMA INITIALIZATION
-- ============================================================

-- Companies table
CREATE TABLE IF NOT EXISTS companies (
    id SERIAL PRIMARY KEY,
    ticker VARCHAR(10) UNIQUE NOT NULL,
    name VARCHAR(255),
    currency VARCHAR(3) DEFAULT 'PLN',
    sector VARCHAR(100),
    shares_outstanding BIGINT,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Quarterly financial data
CREATE TABLE IF NOT EXISTS financials_quarterly (
    id SERIAL PRIMARY KEY,
    ticker VARCHAR(10) NOT NULL REFERENCES companies(ticker) ON DELETE CASCADE,
    rok INT NOT NULL,
    kwartal VARCHAR(3) NOT NULL,
    data_publikacji DATE,
    
    -- P&L
    przychody BIGINT,
    koszty_sprzedanych_produktow BIGINT,
    zysk_brutto_ze_sprzedazy BIGINT,
    koszty_operacyjne BIGINT,
    ebitda BIGINT,
    amortyzacja BIGINT,
    ebit BIGINT,
    przychody_finansowe BIGINT,
    koszty_finansowe BIGINT,
    zysk_brutto BIGINT,
    podatek_dochodowy BIGINT,
    zysk_netto BIGINT,
    
    -- Balance Sheet
    aktywa_obrotowe BIGINT,
    srodki_pieniezne BIGINT,
    naleznosci_krotkoterminowe BIGINT,
    zapasy BIGINT,
    pozostale_aktywa_obrotowe BIGINT,
    aktywa_trwale BIGINT,
    rzeczowe_aktywa_trwale BIGINT,
    wartosci_niematerialne BIGINT,
    inwestycje_dlugoterminowe BIGINT,
    aktywa_razem BIGINT,
    zobowiazania_krotkoterminowe BIGINT,
    dlug_krotkoterminowy BIGINT,
    zobowiazania_dlugoterminowe BIGINT,
    dlug_dlugoterminowy BIGINT,
    kapital_wlasny BIGINT,
    
    -- Cash Flow
    przeplywy_operacyjne BIGINT,
    przeplywy_inwestycyjne BIGINT,
    przeplywy_finansowe BIGINT,
    capex BIGINT,
    free_cash_flow BIGINT,
    
    -- Additional
    dywidenda_na_akcje DECIMAL(10,2),
    liczba_akcji BIGINT,
    
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    
    UNIQUE(ticker, rok, kwartal)
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

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_financials_ticker ON financials_quarterly(ticker);
CREATE INDEX IF NOT EXISTS idx_financials_rok_kwartal ON financials_quarterly(rok, kwartal);
CREATE INDEX IF NOT EXISTS idx_prices_ticker_date ON prices_daily(ticker, date DESC);
CREATE INDEX IF NOT EXISTS idx_prices_date ON prices_daily(date DESC);