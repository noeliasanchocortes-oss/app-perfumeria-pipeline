import os
import re
import json
import time
import random
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
import psycopg2

DB_URL = os.environ["DATABASE_URL"]

BASE = "https://www.parfumo.com"
START_INDEX = "https://www.parfumo.com/Perfumes/Tops/Men"  # MVP index
SOURCE_NAME = "parfumo"
SOURCE_BASE_URL = "https://www.parfumo.com"

MODE = os.environ.get("MODE", "top_men")   # top_men | brand
BRAND = os.environ.get("BRAND", "").strip()
UA = "NuvioPerfumeriaBot/0.1 (respectful; contact: your-email@example.com)"

def norm(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s

def http_get(url: str) -> str:
    time.sleep(1.0 + random.random() * 0.5)
    r = requests.get(url, headers={"User-Agent": UA}, timeout=30)
    r.raise_for_status()
    return r.text

def extract_perfume_urls_from_index(html: str, limit: int) -> list[str]:
    """
    Extrae URLs de perfumes desde páginas tipo Top o Brand.
    Usamos un selector más “semántico” (schema.org/Product) para evitar 0 resultados y evitar menús/footers.
    """
    soup = BeautifulSoup(html, "lxml")
    urls: list[str] = []
    seen: set[str] = set()

    # 1) Intenta primero el contenedor típico con schema.org/Product
    for a in soup.select("div[itemtype='http://schema.org/Product'] a[href]"):
        href = a.get("href", "")
        if not href:
            continue
        if href.startswith("/Perfumes/") or href.startswith("/Parfums/"):
            parts = href.strip("/").split("/")
            if len(parts) >= 3:  # /Perfumes/Brand/slug
                full = urljoin(BASE, href)
                if full not in seen:
                    seen.add(full)
                    urls.append(full)
        if len(urls) >= limit:
            return urls[:limit]

    # 2) Fallback: si no encontró nada, vuelve al método general (por si la página no usa ese contenedor)
    for a in soup.select("a[href]"):
        href = a.get("href", "")
        if not href:
            continue
        if href.startswith("/Perfumes/") or href.startswith("/Parfums/"):
            parts = href.strip("/").split("/")
            if len(parts) >= 3:
                full = urljoin(BASE, href)
                if full not in seen:
                    seen.add(full)
                    urls.append(full)
        if len(urls) >= limit:
            break

    return urls[:limit]

def parse_gender_year_and_name(soup: BeautifulSoup) -> tuple[str | None, int | None, str | None]:
    text = soup.get_text("\n", strip=True)

    year
