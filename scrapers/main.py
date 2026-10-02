import asyncio
import csv
import json
import re
from datetime import datetime, timezone
from urllib.parse import urljoin, urlparse

import aiohttp
from bs4 import BeautifulSoup


USER_AGENT = (
    "Mozilla/5.0 (compatible; RetailerProductCrawler/1.0)"
)

HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept-Language": "nl-NL,nl;q=0.9,en;q=0.8",
}


# ============================================================
# HTTP
# ============================================================

class HttpClient:

    def __init__(self, concurrency=4, delay=0.5):
        self.concurrency = concurrency
        self.delay = delay
        self.semaphore = asyncio.Semaphore(concurrency)
        self.session = None

    async def __aenter__(self):
        connector = aiohttp.TCPConnector(
            limit=self.concurrency
        )

        self.session = aiohttp.ClientSession(
            connector=connector,
            headers=HEADERS
        )

        return self

    async def __aexit__(self, *args):
        await self.session.close()

    async def get(self, url, retries=4):

        async with self.semaphore:

            for attempt in range(retries):

                try:

                    await asyncio.sleep(self.delay)

                    async with self.session.get(
                        url,
                        timeout=aiohttp.ClientTimeout(total=40),
                        allow_redirects=True
                    ) as response:

                        if response.status == 200:
                            return await response.text(
                                errors="ignore"
                            )

                        if response.status in (
                            429,
                            500,
                            502,
                            503,
                            504
                        ):
                            await asyncio.sleep(
                                min(20, 2 ** attempt)
                            )
                            continue

                        print(
                            f"HTTP {response.status}: {url}"
                        )

                        return None

                except (
                    aiohttp.ClientError,
                    asyncio.TimeoutError
                ) as e:

                    print(
                        f"Request error {url}: {e}"
                    )

                    await asyncio.sleep(
                        min(20, 2 ** attempt)
                    )

        return None


# ============================================================
# HELPERS
# ============================================================

def clean(value):

    if value is None:
        return None

    value = re.sub(
        r"\s+",
        " ",
        str(value)
    ).strip()

    return value or None


def price(value):

    if value is None:
        return None

    if isinstance(value, (int, float)):
        return float(value)

    value = str(value)

    value = (
        value
        .replace("€", "")
        .replace("EUR", "")
        .replace(" ", "")
    )

    if "," in value and "." in value:
        value = value.replace(".", "")
        value = value.replace(",", ".")

    else:
        value = value.replace(",", ".")

    match = re.search(
        r"\d+(?:\.\d+)?",
        value
    )

    if not match:
        return None

    return float(match.group())


def first(value):

    if isinstance(value, list):

        if value:
            return value[0]

        return None

    return value


def flatten_jsonld(value):

    if isinstance(value, list):

        result = []

        for item in value:
            result.extend(
                flatten_jsonld(item)
            )

        return result

    if isinstance(value, dict):

        result = [value]

        if "@graph" in value:
            result.extend(
                flatten_jsonld(
                    value["@graph"]
                )
            )

        return result

    return []


# ============================================================
# JSON-LD
# ============================================================

def get_product_jsonld(soup):

    products = []

    for script in soup.find_all(
        "script",
        {
            "type": "application/ld+json"
        }
    ):

        raw = (
            script.string
            or script.get_text()
        )

        try:

            data = json.loads(raw)

        except Exception:

            continue

        for obj in flatten_jsonld(data):

            product_type = obj.get("@type")

            if isinstance(product_type, list):
                types = product_type
            else:
                types = [product_type]

            if "Product" in types:

                products.append(obj)

    if products:
        return products[0]

    return {}


# ============================================================
# META
# ============================================================

def get_meta(soup, *names):

    for name in names:

        element = (
            soup.find(
                "meta",
                attrs={"property": name}
            )
            or
            soup.find(
                "meta",
                attrs={"name": name}
            )
        )

        if element:

            content = element.get("content")

            if content:
                return clean(content)

    return None


# ============================================================
# PRODUCT PARSER
# ============================================================

def parse_product(
    html,
    url,
    retailer
):

    soup = BeautifulSoup(
        html,
        "lxml"
    )

    product = get_product_jsonld(
        soup
    )

    offers = first(
        product.get("offers")
    )

    if not isinstance(
        offers,
        dict
    ):
        offers = {}

    brand = product.get(
        "brand"
    )

    if isinstance(
        brand,
        dict
    ):
        brand = brand.get(
            "name"
        )

    image = first(
        product.get("image")
    )

    if image:
        image = urljoin(
            url,
            image
        )

    else:

        image = get_meta(
            soup,
            "og:image"
        )

    availability = offers.get(
        "availability"
    )

    if availability:

        availability = (
            availability
            .split("/")[-1]
        )

    product_data = {

        "retailer": retailer,

        "product_id": clean(
            product.get(
                "productID"
            )
            or product.get(
                "sku"
            )
            or product.get(
                "mpn"
            )
        ),

        "ean": clean(
            product.get(
                "gtin13"
            )
            or product.get(
                "gtin14"
            )
            or product.get(
                "gtin12"
            )
            or product.get(
                "gtin"
            )
        ),

        "name": clean(
            product.get(
                "name"
            )
            or get_meta(
                soup,
                "og:title",
                "twitter:title"
            )
        ),

        "brand": clean(
            brand
        ),

        "description": clean(
            product.get(
                "description"
            )
            or get_meta(
                soup,
                "description"
            )
        ),

        "category": clean(
            product.get(
                "category"
            )
        ),

        "price": price(
            offers.get(
                "price"
            )
        ),

        "currency": clean(
            offers.get(
                "priceCurrency"
            )
            or "EUR"
        ),

        "availability": clean(
            availability
        ),

        "image_url": image,

        "product_url": url,

        "sku": clean(
            product.get(
                "sku"
            )
        ),

        "gtin": clean(
            product.get(
                "gtin13"
            )
            or product.get(
                "gtin"
            )
        ),

        "raw_jsonld": product,

        "scraped_at":
            datetime.now(
                timezone.utc
            ).isoformat()
    }

    # Extra prijs-fallback
    if product_data["price"] is None:

        text = soup.get_text(
            " ",
            strip=True
        )

        match = re.search(
            r"€\s*(\d+[,.]\d{2})",
            text
        )

        if match:

            product_data["price"] = price(
                match.group(1)
            )

    return product_data


# ============================================================
# SITEMAP
# ============================================================

def extract_sitemap_urls(
    xml
):

    soup = BeautifulSoup(
        xml,
        "xml"
    )

    return [
        loc.get_text(
            strip=True
        )
        for loc in soup.find_all(
            "loc"
        )
    ]


def extract_robot_sitemaps(
    robots
):

    result = []

    for line in robots.splitlines():

        if line.lower().startswith(
            "sitemap:"
        ):

            result.append(
                line.split(
                    ":",
                    1
                )[1].strip()
            )

    return result


async def discover_sitemap_urls(
    http,
    robots_url,
    base_url
):

    robots = await http.get(
        robots_url
    )

    sitemap_urls = []

    if robots:

        sitemap_urls.extend(
            extract_robot_sitemaps(
                robots
            )
        )

    # Algemene fallback
    sitemap_urls.extend([
        urljoin(
            base_url,
            "/sitemap.xml"
        ),
        urljoin(
            base_url,
            "/sitemap_index.xml"
        )
    ])

    sitemap_urls = list(
        dict.fromkeys(
            sitemap_urls
        )
    )

    queue = list(
        sitemap_urls
    )

    visited = set()
    product_urls = set()

    while queue:

        sitemap = queue.pop(0)

        if sitemap in visited:
            continue

        visited.add(
            sitemap
        )

        xml = await http.get(
            sitemap
        )

        if not xml:
            continue

        urls = extract_sitemap_urls(
            xml
        )

        # Sitemap index
        if "<sitemapindex" in xml.lower():

            queue.extend(
                url
                for url in urls
                if url not in visited
            )

        else:

            for url in urls:

                if urlparse(
                    url
                ).netloc.endswith(
                    urlparse(
                        base_url
                    ).netloc
                ):

                    product_urls.add(
                        url
                    )

    return sorted(
        product_urls
    )


# ============================================================
# ALDI
# ============================================================

async def discover_aldi(
    http
):

    urls = await discover_sitemap_urls(
        http,
        "https://www.aldi.nl/robots.txt",
        "https://www.aldi.nl"
    )

    return [
        url
        for url in urls
        if (
            "/producten/"
            in url
            and
            ".article.html"
            in url
        )
    ]


# ============================================================
# KRUIDVAT
# ============================================================

async def discover_kruidvat(
    http
):

    urls = await discover_sitemap_urls(
        http,
        "https://www.kruidvat.nl/robots.txt",
        "https://www.kruidvat.nl"
    )

    return [
        url
        for url in urls
        if "/p/" in url
    ]


# ============================================================
# JUMBO
# ============================================================

async def discover_jumbo(
    http
):

    product_urls = set()

    offset = 0

    page_size = 24

    max_pages = 1000

    for page in range(
        max_pages
    ):

        url = (
            "https://www.jumbo.com/"
            f"producten/?offSet={offset}"
        )

        print(
            f"Jumbo catalogus "
            f"{page + 1}/{max_pages}"
        )

        html = await http.get(
            url
        )

        if not html:
            break

        soup = BeautifulSoup(
            html,
            "lxml"
        )

        before = len(
            product_urls
        )

        for link in soup.find_all(
            "a",
            href=True
        ):

            href = urljoin(
                url,
                link["href"]
            )

            href = href.split(
                "?",
                1
            )[0]

            if (
                "jumbo.com"
                in urlparse(
                    href
                ).netloc
                and
                "/producten/"
                in href
            ):

                product_urls.add(
                    href
                )

        if len(product_urls) == before:
            break

        offset += page_size

    return sorted(
        product_urls
    )


# ============================================================
# ACTION
# ============================================================

async def discover_action(
    http
):

    urls = set()

    # Eerst proberen via sitemap
    sitemap_urls = await discover_sitemap_urls(
        http,
        "https://shop.action.com/robots.txt",
        "https://shop.action.com/nl-nl"
    )

    for url in sitemap_urls:

        if (
            "shop.action.com"
            in url
        ):

            urls.add(
                url
            )

    # Daarna catalogus fallback
    start_urls = [
        "https://shop.action.com/nl-nl",
        "https://shop.action.com/nl-nl/producten"
    ]

    queue = list(
        start_urls
    )

    visited = set()

    while queue and len(
        visited
    ) < 100:

        page = queue.pop(0)

        if page in visited:
            continue

        visited.add(
            page
        )

        html = await http.get(
            page
        )

        if not html:
            continue

        soup = BeautifulSoup(
            html,
            "lxml"
        )

        for link in soup.find_all(
            "a",
            href=True
        ):

            href = urljoin(
                page,
                link["href"]
            )

            if (
                "shop.action.com/nl-nl"
                not in href
            ):
                continue

            path = urlparse(
                href
            ).path

            if (
                "/p/" in path
                or "/product" in path
            ):

                urls.add(
                    href
                )

            elif any(
                x in path
                for x in [
                    "/categorie",
                    "/category",
                    "/producten"
                ]
            ):

                if href not in visited:
                    queue.append(
                        href
                    )

    return sorted(
        urls
    )


# ============================================================
# SCRAPE PRODUCTS
# ============================================================

async def scrape_products(
    http,
    urls,
    retailer,
    limit=0
):

    if limit:
        urls = urls[:limit]

    print(
        f"{retailer}: "
        f"{len(urls)} product URLs"
    )

    products = []

    async def scrape_one(
        url
    ):

        html = await http.get(
            url
        )

        if not html:
            return None

        try:

            product = parse_product(
                html,
                url,
                retailer
            )

            if product.get(
                "name"
            ):
                return product

        except Exception as e:

            print(
                f"Parse error: "
                f"{url}: {e}"
            )

        return None

    batch_size = 20

    for start in range(
        0,
        len(urls),
        batch_size
    ):

        batch = urls[
            start:
            start + batch_size
        ]

        results = await asyncio.gather(
            *[
                scrape_one(url)
                for url in batch
            ]
        )

        products.extend(
            product
            for product in results
            if product
        )

        print(
            f"{retailer}: "
            f"{min(start + batch_size, len(urls))}"
            f"/{len(urls)}"
        )

    # Dedupliceren
    unique = {}

    for product in products:

        key = (
            product.get("ean")
            or product.get("product_id")
            or product.get("product_url")
        )

        unique[key] = product

    return list(
        unique.values()
    )


# ============================================================
# SAVE
# ============================================================

def save_products(
    retailer,
    products
):

    with open(
        f"{retailer}.json",
        "w",
        encoding="utf-8"
    ) as file:

        json.dump(
            products,
            file,
            ensure_ascii=False,
            indent=2
        )

    fields = [
        "retailer",
        "product_id",
        "ean",
        "name",
        "brand",
        "description",
        "category",
        "price",
        "currency",
        "availability",
        "image_url",
        "product_url",
        "sku",
        "gtin",
        "scraped_at"
    ]

    with open(
        f"{retailer}.csv",
        "w",
        newline="",
        encoding="utf-8"
    ) as file:

        writer = csv.DictWriter(
            file,
            fieldnames=fields,
            extrasaction="ignore"
        )

        writer.writeheader()

        writer.writerows(
            products
        )


# ============================================================
# MAIN
# ============================================================

async def main():

    async with HttpClient(
        concurrency=4,
        delay=0.6
    ) as http:

        retailers = {

            "aldi":
                discover_aldi,

            "jumbo":
                discover_jumbo,

            "action":
                discover_action,

            "kruidvat":
                discover_kruidvat
        }

        for (
            retailer,
            discover
        ) in retailers.items():

            print()
            print(
                "=" * 60
            )
            print(
                retailer.upper()
            )
            print(
                "=" * 60
            )

            try:

                urls = await discover(
                    http
                )

                print(
                    f"{len(urls)} "
                    f"productpagina's gevonden"
                )

                products = await scrape_products(
                    http,
                    urls,
                    retailer
                )

                print(
                    f"{len(products)} "
                    f"producten verzameld"
                )

                save_products(
                    retailer,
                    products
                )

            except Exception as e:

                print(
                    f"{retailer} mislukt: "
                    f"{e}"
                )


if __name__ == "__main__":

    asyncio.run(
        main()
    )
