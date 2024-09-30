"""Top-level package for fin-chatbot."""

__author__ = """Maryam K"""
__email__ = 'audreyr@example.com'
__version__ = '0.1.0'
from fin_chatbot.api import create_app

app = create_app()

if __name__ == "__main__":
    app.run(debug=True)