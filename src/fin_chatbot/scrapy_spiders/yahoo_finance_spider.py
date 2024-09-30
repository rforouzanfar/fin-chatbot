import scrapy

class FinancialSpider(scrapy.Spider):
    name = "financial_spider"
    start_urls = [
        'https://www.bloomberg.com/markets/stocks',
        'https://www.reuters.com/markets'
    ]

    def parse(self, response):
        for article in response.xpath('//article'):
            yield {
                'title': article.xpath('.//h3/a/text()').get(),
                'url': article.xpath('.//h3/a/@href').get(),
                'summary': article.xpath('.//p/text()').get(),
            }
