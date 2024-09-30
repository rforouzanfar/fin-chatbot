BOT_NAME = 'fin_chatbot_spider'

SPIDER_MODULES = ['fin_chatbot.scrapy_spiders']
NEWSPIDER_MODULE = 'fin_chatbot.scrapy_spiders'

# Additional settings (if needed):
# USER_AGENT = 'your_custom_user_agent'
ROBOTSTXT_OBEY = True
REQUEST_FINGERPRINTER_IMPLEMENTATION = '2.7'
