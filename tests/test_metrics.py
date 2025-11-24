import unittest
from app.utils.metrics import MetricsCalculator

class TestMetrics(unittest.TestCase):
    def test_pe_ratio(self):
        # Given
        price_data = {'close': 100}
        financial_data = {'zysk_netto': 1000, 'liczba_akcji': 100} # EPS = 10
        
        # When
        calc = MetricsCalculator(price_data, financial_data)
        pe = calc.calculate_pe_ratio()
        
        # Then
        self.assertEqual(pe, 10.0)  # Price(100) / EPS(10) = 10

if __name__ == '__main__':
    unittest.main()