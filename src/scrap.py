import asyncio
import aiohttp
import aiofiles
from fake_useragent import UserAgent
import random
import logging


CITY_ID = "0c5b2444-70a0-4932-980c-b4dc0d3f02b5"  # Moscow

ua = UserAgent(
    browsers=["edge", "chrome", "firefox", "safari"],
    os=["windows", "macos", "ios", "android"],
)


class FileManager:
    @classmethod
    async def write_product_data(cls, data: list):
        async with aiofiles.open("products.jsonl", mode="r") as f:
            json_data = await f.readlines()
        if len(json_data) > 0:
            content = json_data
            async with aiofiles.open("products.jsonl", mode="w+") as f:
                for _ in data:
                    content.append(_)
                for _ in content:
                    await f.writelines(str(_).strip() + "\n")
        else:
            async with aiofiles.open("products.jsonl", mode="w+") as f:
                for _ in data:
                    await f.writelines(str(_) + "\n")


class GoldenApleApi:

    # Moscow
    cityId = CITY_ID

    @classmethod
    async def fetch_category_products_data(cls, categoryId: int):
        category_products: list = []
        logging.info(f'{categoryId} start scrapping')
        for page_number in range(1, 501):
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
                            raise ValueError(
                                f"Status code not 200 {categoryId=} : {page_number=}"
                            )

                        data = resp.get("data")
                        products = data.get("products")
                        for product in products:

                            photo_format = product.get("imageUrls")[0].get(
                                "format"
                            )[-1]
                            screen = product.get("imageUrls")[0].get("screen")[
                                -1
                            ]
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
                                "price": product.get("price")
                                .get("actual")
                                .get("amount"),
                            }
                            category_products.append(product_data)
                logging.info(f'{page_number=} : ok')
            except ValueError as e:
                await asyncio.sleep(random.randint(3, 7))
                logging.warning(e)
        return category_products


async def main():
    logging.basicConfig(level=logging.INFO, filename="py_log.log",filemode="w", format="%(asctime)s %(levelname)s %(message)s")
    ans = await GoldenApleApi.fetch_category_products_data(
        categoryId=1000000003,
    )

    await FileManager.write_product_data(data=ans)


asyncio.run(main())
