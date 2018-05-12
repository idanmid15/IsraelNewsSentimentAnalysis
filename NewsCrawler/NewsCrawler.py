from scrapy.crawler import CrawlerProcess
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from bs4 import BeautifulSoup
import time
from multiprocessing import Process
import urllib
import SqsQueueClient
import RawDataStream
import SimpleHealthServer
from concurrent.futures import ThreadPoolExecutor
pool = ThreadPoolExecutor(8)
class NewsCrawler(CrawlSpider):

    name = 'newscrawler'
    custom_settings = {
        'DEPTH_LIMIT': 1
    }
    rules = (
        Rule(LinkExtractor(), callback='parse_item', follow=True),
    )

    def parse_item(self, response):
        text = self.clean_text(response)
        RawDataStream.send_raw_text(response.url, response.url + ", " + text, pool)

    def clean_text(self, response):
        soup = BeautifulSoup(response.body, "html.parser")
        for script in soup(["script", "style"]):
            script.extract()  # rip it out

        # get text
        new_text = soup.get_text().encode('utf-8').strip()
        # break into lines and remove leading and trailing space on each
        lines = (line.strip() for line in new_text.splitlines())
        # break multi-headlines into a line each
        chunks = (phrase.strip() for line in lines for phrase in line.split("  "))

        # drop blank lines
        chunks_existing = (chunk for chunk in chunks if chunk)
        text = '.\n'.join(chunk for chunk in chunks_existing if chunk)
        return text

def run_crawler(crawler, start_url):
    process = CrawlerProcess({
        'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)'
    })
    process.crawl(crawler, start_urls = [start_url])
    process.start()

def main():
    from threading import Thread
    Thread(target=SimpleHealthServer.run).start()
    while True:
        try:
            start_url, receipt_handle = SqsQueueClient.receive_next_url()
            p = Process(target=run_crawler, args=(NewsCrawler, start_url))
            p.start()
            p.join()
            SqsQueueClient.delete_received_url(receipt_handle)
        except Exception as e:
            print e

if __name__ == "__main__":
    main()