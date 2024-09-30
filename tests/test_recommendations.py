import unittest
from fin_chatbot.services.recommendations import generate_recommendations

class TestRecommendations(unittest.TestCase):
    def test_recommendations_long_term(self):
        result = generate_recommendations(10000, 5)
        self.assertIn('AAPL', result['recommendation'])

    def test_recommendations_short_term(self):
        result = generate_recommendations(5000, 2)
        self.assertIn('bonds', result['recommendation'])
