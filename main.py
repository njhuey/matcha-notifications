from typing import TypedDict
from datetime import datetime
from pathlib import Path
import requests

from bs4 import BeautifulSoup
import pandas as pd
import duckdb

MATCHA_PRODUCTS = [
    "https://www.marukyu-koyamaen.co.jp/english/shop/products/1161020c1/",
    "https://www.marukyu-koyamaen.co.jp/english/shop/products/1171020c1/",
    "https://www.marukyu-koyamaen.co.jp/english/shop/products/1191040c1/",
]

DB_PATH = Path("matcha-products.ddb")


class ProductSize(TypedDict):
    name: str
    size: str
    available: bool


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
                "name": product_name,
                "size": size,
                "available": "out-of-stock" not in str(div),
            }
        )

    return pd.DataFrame(product_sizes)


def update_product(product_status: pd.Series, conn: duckdb.DuckDBPyConnection) -> bool:
    """
    Update product with latest data scraped from the web.

    Returns
    -------
    bool : True if the status of the item has become available since the last run.
    """
    row = conn.execute(
        """
SELECT available
FROM product
WHERE name = ? AND size = ?;
""",
        (product_status["name"], product_status["size"]),
    ).fetchone()

    newly_available = False
    match row:
        case (previously_available,):
            if not previously_available and product_status["available"]:
                newly_available = True
            conn.execute(
                """
UPDATE product
SET available = ?, last_modified = ?
WHERE name = ? AND size = ?;
""",
                (product_status["available", datetime.now()]),
            )
        case None:
            conn.execute(
                """
INSERT INTO product (name, size, available, created_date, last_modified)
VALUES (?, ?, ?, ?, ?);
""",
                (
                    product_status["name"],
                    product_status["size"],
                    product_status["available"],
                    datetime.now(),
                    datetime.now(),
                ),
            )
        case _:
            raise ValueError(
                "When accessing `product` table, received a non-sensical response. Exiting."  # NOQA: E501
            )
    return newly_available


def track_availibility(product_statuses: pd.DataFrame) -> pd.DataFrame:
    """
    Track the changes in availibility of each matcha product size combination.

    Uses `duckdb` to inspect when an unavailable item becomes available and returns a
    `DataFrame` with the newly available items.
    """
    conn = duckdb.connect(str(DB_PATH))

    conn.query(
        """
CREATE TABLE IF NOT EXISTS product (
    name VARCHAR,
    size VARCHAR,
    available BOOL,
    created_date TIME,
    last_modified TIME,
    PRIMARY KEY (name, size)
);
"""
    )

    newly_available_products = []
    for _, product in product_statuses.iterrows():
        became_available = update_product(product, conn)
        if became_available:
            newly_available_products.append(product)

    return pd.DataFrame(newly_available_products)


def main() -> None:
    product_dfs = [
        scrape_matcha_availability(product_url) for product_url in MATCHA_PRODUCTS
    ]
    product_df = pd.concat(product_dfs)

    newly_available_products = track_availibility(product_df)
    if len(newly_available_products) == 0:
        return

    print(newly_available_products)


if __name__ == "__main__":
    main()
