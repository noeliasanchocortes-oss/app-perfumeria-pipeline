import os
import re
import json
import time
import random
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
import psycopg2

# =========================
# CONFIG
# =========================
DB_URL = os.environ["DATABASE_URL"]

BASE = "https://www.parfumo.com"
SOURCE_NAME = "parfumo"
SOURCE_BASE_URL = BASE

START_URL = os.environ.get(
    "START_URL",
    "https://www.parfumo.com/Perfumes/Tops/Men"
).strip()

LIMIT = int(os.environ.get("LIMIT", "20"))
MAX_PAGES = int(os.environ.get("MAX_PAGES", "3"))

UA = "NuvioPerfumeriaBot/0.1 (respectful; contact: you@example.com)"

# =========================
# HELPERS
# =========================
def norm(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip().lower())

def http_get(url: str) -> str:
    time.sleep(1 + random.random())
    r = requests.get(url, headers={"User-Agent": UA}, timeout=30)
    r.raise_for_status()
    return r.text

def page_url(base_url: str, page: int) -> str:
    if page <= 1:
        return base_url
    sep = "&" if "?" in base_url else "?"
    return f"{base_url}{sep}page={page}"

# =========================
# URL EXTRACTION (FIX REAL)
# =========================
def extract_perfume_urls_from_listing(html: str, limit: int) -> list[str]:
    soup = BeautifulSoup(html, "lxml")
    urls = []
    seen = set()

    for a in soup.select("a[href]"):
        href = (a.get("href") or "").strip()
        if not href:
            continue

        # Absoluto → path
        if href.startswith("http"):
            m = re.search(r"https?://[^/]+(/.*)", href)
            if not m:
                continue
            path = m.group(1)
        else:
            path = href

        # Aceptamos EN / FR
        if not (path.startswith("/Perfumes/") or path.startswith("/Parfums/")):
            continue

        parts = path.strip("/").split("/")
        if len(parts) < 3:
            continue

        _, brand, slug = parts[:3]
        if not brand or not slug:
            continue

        if slug.lower() in {
            "tops", "top", "new", "ratings", "reviews",
            "images", "perfumes", "parfums"
        }:
            continue

        full = urljoin(BASE, path)
        if full in seen:
            continue

        seen.add(full)
        urls.append(full)

        if len(urls) >= limit:
            break

    return urls

# =========================
# PARSING PERFUME PAGE
# =========================
def parse_brand_from_url(url: str) -> str:
    parts = url.split("/")
    i = parts.index("Perfumes") if "Perfumes" in parts else parts.index("Parfums")
    return parts[i + 1].replace("_", " ")

def parse_perfume_page(url: str) -> dict:
    html = http_get(url)
    soup = BeautifulSoup(html, "lxml")

    name = soup.select_one("h1")
    name = name.get_text(strip=True) if name else url.split("/")[-1]

    text = soup.get_text("\n", strip=True)

    year = None
    m = re.search(r"(19|20)\d{2}", text)
    if m:
        year = int(m.group(0))

    gender = None
    if "for men" in text.lower():
        gender = "male"
    elif "for women" in text.lower():
        gender = "female"
    elif "for women and men" in text.lower():
        gender = "unisex"

    return {
        "url": url,
        "brand": parse_brand_from_url(url),
        "name": name,
        "year": year,
        "gender": gender,
        "concentration": None,
        "perfumers": [],
        "notes": [],
    }

# =========================
# DB UPSERT
# =========================
def db_upsert(cur, data: dict) -> int:
    cur.execute(
        """
        INSERT INTO source (name, base_url, reliability)
        VALUES (%s, %s, 80)
        ON CONFLICT (name) DO UPDATE SET base_url=EXCLUDED.base_url
        RETURNING id
        """,
        (SOURCE_NAME, SOURCE_BASE_URL),
    )
    source_id = cur.fetchone()[0]

    cur.execute(
        """
        INSERT INTO brand (name, name_norm)
        VALUES (%s, %s)
        ON CONFLICT (name_norm) DO UPDATE SET name=EXCLUDED.name
        RETURNING id
        """,
        (data["brand"], norm(data["brand"])),
    )
    brand_id = cur.fetchone()[0]

    cur.execute(
        """
        INSERT INTO perfume (brand_id, name, name_norm, year, gender, concentration)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (brand_id, name_norm)
        DO UPDATE SET year=EXCLUDED.year, gender=EXCLUDED.gender
        RETURNING id
        """,
        (
            brand_id,
            data["name"],
            norm(data["name"]),
            data["year"],
            data["gender"],
            data["concentration"],
        ),
    )
    perfume_id = cur.fetchone()[0]

    cur.execute(
        """
        INSERT INTO perfume_source (perfume_id, source_id, url, raw_json)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (perfume_id, source_id)
        DO UPDATE SET url=EXCLUDED.url, raw_json=EXCLUDED.raw_json
        """,
        (
            perfume_id,
            source_id,
            data["url"],
            json.dumps(data, ensure_ascii=False),
        ),
    )

    return perfume_id

# =========================
# MAIN
# =========================
def main():
    print(f"START_URL = {START_URL}")
    print(f"MAX_PAGES = {MAX_PAGES} | LIMIT = {LIMIT}")

    all_urls = []
    seen = set()

    for page in range(1, MAX_PAGES + 1):
        url = page_url(START_URL, page)
        html = http_get(url)
        batch = extract_perfume_urls_from_listing(html, LIMIT)

        new = [u for u in batch if u not in seen]
        for u in new:
            seen.add(u)
            all_urls.append(u)

        print(f"Page {page}: encontradas {len(batch)} (nuevas {len(new)}). Total: {len(all_urls)}")

        if not new or len(all_urls) >= LIMIT:
            break

    if not all_urls:
        print("AVISO: 0 URLs totales. El listado no contiene perfumes estáticos.")
        return

    with psycopg2.connect(DB_URL) as conn:
        with conn.cursor() as cur:
            for i, u in enumerate(all_urls, 1):
                try:
                    data = parse_perfume_page(u)
                    pid = db_upsert(cur, data)
                    print(f"OK [{i}/{len(all_urls)}] perfume_id={pid}")
                except Exception as e:
                    print(f"ERROR en {u}: {e}")
            conn.commit()

    print("DONE.")

if __name__ == "__main__":
    main()
