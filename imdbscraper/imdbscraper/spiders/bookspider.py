import scrapy


class BookspiderSpider(scrapy.Spider):
    name = "bookspider"
    allowed_domains = ["imdb.com"]
    start_urls = ["https://imdb.com"]

    def parse(self, response):
        pass
