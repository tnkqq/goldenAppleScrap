import asyncio
import logging
import os
import random
import re
import sys

import aiofiles
import aiofiles.os
import aiohttp
from bs4 import BeautifulSoup
from fake_useragent import UserAgent

PAGE_FROM_CATEGORY = 5  # max500

CITY_ID = "0c5b2444-70a0-4932-980c-b4dc0d3f02b5"  # Moscow

URLS = [
    "https://goldapple.ru/makijazh",
    "https://goldapple.ru/uhod",
    "https://goldapple.ru/parfjumerija",
    "https://goldapple.ru/volosy",
    "https://goldapple.ru/azija",
    "https://goldapple.ru/tehnika",
    "https://goldapple.ru/dlja-doma",
    "https://goldapple.ru/ukrashenija",
    "https://goldapple.ru/odezhda-i-aksessuary",
    "https://goldapple.ru/aptechnaja-kosmetika/bady",
    "https://goldapple.ru/aptechnaja-kosmetika",
    "https://goldapple.ru/makijazh/nogti",
    "https://goldapple.ru/organika",
    "https://goldapple.ru/uborki-i-gigiena",
    "https://goldapple.ru/detjam",
    "https://goldapple.ru/dlja-muzhchin",
]

ua = UserAgent(
    browsers=["edge", "chrome", "firefox", "safari"],
    os=["windows", "macos", "ios", "android"],
)


class FileManager:
    '''
    create goldenAplleData/ dir 
    create categories.jsonl files  
    '''
    @classmethod
    async def write_product_data(cls, data: list, prefix: str) -> None:

        if not os.path.exists("goldenAplleData/"):
            await aiofiles.os.makedirs("goldenAplleData/")
        if not os.path.exists(f"{prefix}-products.jsonl"):
            await aiofiles.open(f"goldenAplleData/{prefix}-products.jsonl", mode="x")

        async with aiofiles.open(f"goldenAplleData/{prefix}-products.jsonl", mode="r") as f:
            json_data = await f.readlines()

        if len(json_data) > 0:
            content = json_data
            async with aiofiles.open(f"goldenAplleData/{prefix}-products.jsonl", mode="w+") as f:
                for _ in data:
                    content.append(_)
                for _ in content:
                    await f.writelines(str(_).strip() + "\n")
        else:
            async with aiofiles.open(f"goldenAplleData/{prefix}-products.jsonl", mode="w+") as f:
                for _ in data:
                    await f.writelines(str(_) + "\n")


class GoldenApleApi:

    # Moscow
    cityId = CITY_ID
    urls = URLS

    @classmethod
    async def get_categories_id(
        cls,
    ) -> list[int]:
        '''get category id from category page'''
        pattern = r"/p/c/(\d+)/"
        categories_url = []
        for url in cls.urls:
            try:
                async with aiohttp.ClientSession(headers={"user-agent": ua.random}) as session:
                    async with session.get(url) as response:
                        resp = await response.text()
                        soup = BeautifulSoup(resp, "lxml")
                        meta_tags = soup.find_all("meta")
                        for meta in meta_tags:
                            content = meta.get("content")
                            if content and "/p/c/" in content:
                                match = re.search(pattern, content)
                                if match:
                                    category_id = match.group(1)
                                    logging.info(f"category {url.split('/')[-1]} - id : {category_id}")
                                    categories_url.append(int(category_id))
                                else:
                                    logging.warning(f"category {url.split('/')[-1]} - id : Not Found")
            except:
                logging.error("get categories id error")
        return categories_url

    @classmethod
    async def fetch_category_products_data(cls, categoryId: int):
        '''get products from pages each category'''
        category_products: list = []
        logging.info(f"{categoryId} start scrapping")
        for page_number in range(1, PAGE_FROM_CATEGORY+1):
            params = {
                "categoryId": categoryId,
                "cityId": cls.cityId,
                "pageNumber": page_number,
            }

            try:
                async with aiohttp.ClientSession(
                    headers={"user-agent": ua.random},
                    fallback_charset_resolver="utf-8",
                ) as session:
                    url = "https://goldapple.ru/front/api/catalog/products"
                    async with session.get(url, params=params) as response:
                        resp = await response.json()
                        if response.status != 200 or resp.get("data") is None:
                            raise ValueError(f"Status code not 200 {categoryId=} : {page_number=}")

                        data = resp.get("data")
                        products = data.get("products")
                        for product in products:

                            photo_format = product.get("imageUrls")[0].get("format")[-1]
                            screen = product.get("imageUrls")[0].get("screen")[-1]
                            photo_url = (
                                product.get("imageUrls")[0]
                                .get("url")
                                .replace("${screen}", screen)
                                .replace("${format}", photo_format)
                            )

                            product_data = {
                                "id": product.get("itemId"),
                                "name": product.get("name"),
                                "brand": product.get("brand"),
                                "type": product.get("productType"),
                                "photo": photo_url,
                                "in_stock": product.get("inStock"),
                                "price": product.get("price").get("actual").get("amount"),
                            }

                            category_products.append(product_data)
                logging.info(f"{page_number=} : ok")

            except ValueError as e:
                await asyncio.sleep(random.randint(1, 2))
                logging.warning(e)

        return category_products


async def main():
    logging.basicConfig(
        level=logging.INFO,
        filemode="w",
        format="%(asctime)s %(levelname)s %(message)s",
        stream=sys.stdout,
    )

    categories_id = await GoldenApleApi.get_categories_id()
    for index, category in enumerate(categories_id):
        data = await GoldenApleApi.fetch_category_products_data(
            categoryId=category,
        )
        await FileManager.write_product_data(data=data, prefix=URLS[index].split("/")[-1])


asyncio.run(main())
