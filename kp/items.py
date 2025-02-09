# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class KpItem(scrapy.Item):
    name = scrapy.Field()  # Название точки
    mcc_code = scrapy.Field()  # MCC-код
    address = scrapy.Field()  # Адрес
    coordinates = scrapy.Field()  # Координаты
    organization_name = scrapy.Field()  # Название организации
    organization_description = scrapy.Field()  # Описание организации
    source_url = scrapy.Field()  # Ссылка на источник данных
    header_photo_url = scrapy.Field()
