from dotenv import load_dotenv
import os

load_dotenv()  # Load environment variables from .env file

class Config:
    API_KEY = os.getenv('AAMJPNV6FOJ0YMCO')
