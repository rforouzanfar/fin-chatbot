import unittest
from src.my_python_project.api import create_app

class TestAPI(unittest.TestCase):
    def setUp(self):
        self.app = create_app()
        self.client = self.app.test_client()

    def test_investment_endpoint(self):
        response = self.client.post('/invest', json={'amount': 10000, 'period': 5})
        self.assertEqual(response.status_code, 200)
        self.assertIn('recommendation', response.get_json())
