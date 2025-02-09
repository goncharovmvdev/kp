import base64
from io import BytesIO

import aiohttp
from PIL import Image
from aiohttp.client_exceptions import InvalidUrlClientError


class PhotoDownloaderPipeline:
    def __init__(self, result_image_quality: int):
        self.result_image_quality = result_image_quality

    @classmethod
    def from_crawler(cls, crawler):
        result_image_quality = crawler.settings.get("RESULT_IMAGE_QUALITY", 35)
        return cls(result_image_quality=result_image_quality)

    def compress_image(self, image_content: bytes):
        input_buffer = BytesIO(image_content)
        output_buffer = BytesIO()
        img = Image.open(input_buffer)
        if img.mode in ("RGBA", "P"):
            img = img.convert("RGB")
        img.save(output_buffer, format="JPEG", quality=self.result_image_quality, optimize=True)
        return output_buffer.getvalue()

    async def _download_photo_to_base64(self, url: str):
        async with aiohttp.ClientSession() as session:
            response = await session.get(url)
            if response.status != 200:
                return ""
            content = await response.read()
            compressed_bytes = self.compress_image(image_content=content)
            encoded_image = base64.b64encode(compressed_bytes).decode("utf-8")
            return encoded_image

    async def process_item(self, item, spider):
        if item["header_photo_url"]:
            try:
                photo_base64 = await self._download_photo_to_base64(item["header_photo_url"])
            except InvalidUrlClientError:
                item["header_photo_url"] = None
                return item
            item["header_photo_base64"] = photo_base64
            return item
        return item


import psycopg2
from psycopg2.extras import Json


# НИКАКОЙ МОНГИ! ТОЛЬКО ПРАВОСЛАВНАЯ ПОСТГРЯ!!!
# С монгой были проблемы, а с постгрей было все сильно проще + jsonb выручает + мне не нравится тулинг для монги
class PostgresPipeline:
    def __init__(self, postgres_uri, postgres_user, postgres_password, postgres_db):
        self.postgres_uri = postgres_uri
        self.postgres_user = postgres_user
        self.postgres_password = postgres_password
        self.postgres_db = postgres_db

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            postgres_uri=crawler.settings.get('POSTGRES_URI'),
            postgres_user=crawler.settings.get('POSTGRES_USER'),
            postgres_password=crawler.settings.get('POSTGRES_PASSWORD'),
            postgres_db=crawler.settings.get('POSTGRES_DB')
        )

    def open_spider(self, spider):
        self.connection = psycopg2.connect(
            host=self.postgres_uri,
            user=self.postgres_user,
            password=self.postgres_password,
            dbname=self.postgres_db
        )
        self.cursor = self.connection.cursor()

    def close_spider(self, spider):
        self.cursor.close()
        self.connection.commit()
        self.connection.close()

    def process_item(self, item, spider):
        self.cursor.execute(
            """
            INSERT INTO scrapy_items (data) VALUES (%s)
            """,
            [Json(dict(item))]
        )
        self.connection.commit()
        return item
