import json

import scrapy
# from itemloaders import ItemLoader
# from itemloaders.processors import TakeFirst, MapCompose
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings


class ArticleItem(scrapy.Item):
    title = scrapy.Field()
    description = scrapy.Field()
    article_text = scrapy.Field()
    publication_datetime = scrapy.Field()
    header_photo_url = scrapy.Field()
    keywords = scrapy.Field()
    source_url = scrapy.Field()
    header_photo_base64 = scrapy.Field()


class KpSpider(scrapy.spiders.SitemapSpider):
    name = "kp_spider"
    custom_settings = {
        "PLAYWRIGHT_BROWSER_TYPE": "chromium",
        "PLAYWRIGHT_LAUNCH_OPTIONS": {
            "headless": True
        },
        "DOWNLOAD_HANDLERS": {
            "http": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
            "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
        }
    }

    # во всех остальных сайтмэпсах какой-то мусор, так что тут хардкод вроде как неплохое решение - агрессивно сканируем только то что надо
    sitemap_urls = [
        "https://www.kp.ru/sitemap/main_01.xml",
        "https://www.kp.ru/sitemap/main_02.xml",
        "https://www.kp.ru/sitemap/main_03.xml",
        #
        "https://www.kp.ru/sitemap/news_01.xml",
        "https://www.kp.ru/sitemap/news_02.xml"
    ]

    def parse(self, response, **kwargs):
        # в помойку xpath :)
        item = ArticleItem()
        item['title'] = response.css('meta[property="og:title"]::attr(content)').get()
        item['description'] = response.css('meta[name="description"]::attr(content)').get()
        item['article_text'] = self.__article_text(response)
        item['publication_datetime'] = response.css('meta[property="article:published_time"]::attr(content)').get()
        item['header_photo_url'] = response.css('meta[property="og:image"]::attr(content)').get()
        item['keywords'] = response.css('meta[name="keywords"]::attr(content)').get()
        item['source_url'] = response.url
        yield item

    @staticmethod
    def __article_text(response):
        script_content = response.css('script::text').re_first(r'window\.__PRELOADED_STATE__ = ({.*?});', default='{}')
        preloaded_data = json.loads(script_content)
        texts = []

        # https://www.youtube.com/watch?v=JVDg8DzD46I )))))))))))
        def find_paragraphs(obj):
            if isinstance(obj, dict):
                if obj.get("@context") == "paragraph" and "text" in obj.get("ru", {}):
                    texts.append(obj["ru"]["text"])
                for key, value in obj.items():
                    find_paragraphs(value)
            elif isinstance(obj, list):
                for item in obj:
                    find_paragraphs(item)

        find_paragraphs(preloaded_data)
        return '. '.join(texts)


if __name__ == "__main__":
    process = CrawlerProcess(get_project_settings())
    process.crawl(KpSpider)
    process.start()
