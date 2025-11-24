-- Insert initial company data example
INSERT INTO companies (ticker, name, currency, sector)
VALUES 
    ('TXT.WA', 'Text SA', 'PLN', 'IT'),
    ('PCR.WA', 'PCC Rokita SA', 'PLN', 'Chemicals'),
    ('RBW.WA', 'Rainbow Tours SA', 'PLN', 'Services'),
    ('PFE', 'Pfizer', 'USD', 'Healthcare')
ON CONFLICT (ticker) DO NOTHING;