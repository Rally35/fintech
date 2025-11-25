-- Insert initial company data example
INSERT INTO companies (ticker, name, currency, sector, industry)
VALUES 
    ('TXT.WA', 'Text SA', 'PLN', 'IT', 'IT'),
    ('PCR.WA', 'PCC Rokita SA', 'PLN', 'Chemicals', 'Chemicals'),
    ('RBW.WA', 'Rainbow Tours SA', 'PLN', 'Services', 'Services'),
    ('PFE', 'Pfizer', 'USD', 'Healthcare', 'Services')
ON CONFLICT (ticker) DO NOTHING;