from typing import TypedDict
import requests

from bs4 import BeautifulSoup
import pandas as pd

MATCHA_PRODUCTS = [
    "https://www.marukyu-koyamaen.co.jp/english/shop/products/1161020c1/",
    "https://www.marukyu-koyamaen.co.jp/english/shop/products/1171020c1/",
    "https://www.marukyu-koyamaen.co.jp/english/shop/products/1191040c1/",
]


class ProductSize(TypedDict):
    product_name: str
    size: str
    out_of_stock: bool


def scrape_matcha_availability(product: str) -> pd.DataFrame:
    """Scrape matcha product sizes and availability."""
    response = requests.get(product)

    page = BeautifulSoup(response.text, features="html.parser")

    product_name = page.title.text.split()[0]

    divs = page.find_all("div", "product-form-row")
    product_sizes: list[ProductSize] = []
    for div in divs:
        size = div.find("dl", "pa-pa_size").dd.text

        if size is None or type(size) is not str:
            raise ValueError(f"No size available for product `{product_name}`.")

        product_sizes.append(
            {
                "product_name": product_name,
                "size": size,
                "out_of_stock": "out-of-stock" in str(div),
            }
        )

    return pd.DataFrame(product_sizes)


def main() -> None:
    product_dfs = [
        scrape_matcha_availability(product_url) for product_url in MATCHA_PRODUCTS
    ]
    product_df = pd.concat(product_dfs)

    available = product_df[~product_df["out_of_stock"]]
    if len(available) == 0:
        return

    message = f"""
Available matcha flavors

{str(available[["product_name", "size"]])}
"""
    print(message)


if __name__ == "__main__":
    main()
