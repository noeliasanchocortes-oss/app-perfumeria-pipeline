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

SOURCE_NAME = "parfumo"
SOURCE_BASE_URL = "https://www.parfumo.com"

BASE = "https://www.parfumo.com"
START_URL = os.environ.get("START_URL", "https://www.parfumo.com/Perfumes/Tops/Men").strip()
LIMIT = int(os.environ.get("LIMIT", "20"))
MAX_PAGES = int(os.environ.get("MAX_PAGES", "5"))

UA = "NuvioPerfumeriaBot/0.1 (respectful; contact: your-email@example.com)"

def norm(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s

def http_get(url: str) -> str:
    time.sleep(1.0 + random.random() * 0.6)
    r = requests.get(url, headers={"User-Agent": UA}, timeout=30)
    r.raise_for_status()
    return r.text

def page_url(base_url: str, page: int) -> str:
    if page <= 1:
        return base_url
    sep = "&" if "?" in base_url else "?"
    return f"{base_url}{sep}page={page}"

def extract_perfume_urls_from_listing(html: str, limit: int) -> list[str]:
    soup = BeautifulSoup(html, "lxml")
    urls: list[str] = []
    seen: set[str] = set()

    for a in soup.select("a[href]"):
        href = (a.get("href") or "").strip()
        if not href.startswith("/Perfumes/"):
            continue

        parts = href.strip("/").split("/")
        if len(parts) < 3:
            continue

        _, brand, slug = parts[:3]
        if not brand or not slug:
            continue
        if slug.lower() in {"tops", "new", "ratings", "reviews", "images"}:
            continue

        full = urljoin(BASE, href)
        if full in seen:
            continue

        seen.add(full)
        urls.append(full)

        if len(urls) >= limit:
            break

    return urls

def parse_brand_from_url(url: str) -> str:
    parts = url.split("/")
    idx = parts.index("Perfumes")
    brand_slug = parts[idx + 1]
    return brand_slug.replace("_", " ").strip()

def parse_gender_year_and_name(soup: BeautifulSoup) -> tuple[str | None, int | None, str | None]:
    text = soup.get_text("\n", strip=True)

    year = None
    m = re.search(r"released in\s+(\d{4})", text, re.IGNORECASE)
    if m:
        year = int(m.group(1))
    else:
        m2 = re.search(r"\b(19|20)\d{2}\b", text)
        if m2:
            year = int(m2.group(0))

    gender = None
    if re.search(r"for women and men|for men and women", text, re.IGNORECASE):
        gender = "unisex"
    elif re.search(r"\bfor men\b", text, re.IGNORECASE):
        gender = "male"
    elif re.search(r"\bfor women\b", text, re.IGNORECASE):
        gender = "female"

    name = None
    h1 = soup.select_one("h1")
    if h1:
        name = h1.get_text(" ", strip=True)

    return gender, year, name

def parse_notes_and_perfumers(soup: BeautifulSoup) -> tuple[list[tuple[str, str]], list[str]]:
    text = soup.get_text("\n", strip=True)
    lines = [ln.strip() for ln in text.split("\n") if ln.strip()]

    perfumers: list[str] = []
    for i, line in enumerate(lines):
        if line.lower() == "perfumer":
            j = i + 1
            while j < len(lines):
                nxt = lines[j].strip()
                low = nxt.lower()
                if low in {"ratings", "rating", "fragrance pyramid", "top notes", "heart notes", "base notes"}:
                    break
                if len(nxt) <= 80:
                    perfumers.append(nxt)
                j += 1
            break
    perfumers = list(dict.fromkeys([p for p in perfumers if p]))

    notes: list[tuple[str, str]] = []

    def collect_after(label: str, pos: str):
        if label not in lines:
            return
        idx = lines.index(label)
        j = idx + 1
        while j < len(lines):
            nxt = lines[j].strip()
            low = nxt.lower()
            if low in {"top notes", "heart notes", "base notes", "perfumer", "ratings", "rating"}:
                break
            if len(nxt) <= 40:
                notes.append((nxt, pos))
            j += 1

    if "Top Notes" in lines:
        collect_after("Top Notes", "top")
    if "Heart Notes" in lines:
        collect_after("Heart Notes", "heart")
    if "Base Notes" in lines:
        collect_after("Base Notes", "base")

    seen = set()
    clean_notes = []
    for n, pos in notes:
        key = (norm(n), pos)
        if key not in seen:
            seen.add(key)
            clean_notes.append((n, pos))

    return clean_notes, perfumers

def db_upsert(cur, data: dict) -> int:
    cur.execute(
        "INSERT INTO source(name, base_url, reliability) "
        "VALUES (%s, %s, 80) "
        "ON CONFLICT (name) DO UPDATE SET base_url=EXCLUDED.base_url "
        "RETURNING id",
        (SOURCE_NAME, SOURCE_BASE_URL),
    )
    source_id = cur.fetchone()[0]

    cur.execute(
        "INSERT INTO brand(name, name_norm) "
        "VALUES (%s, %s) "
        "ON CONFLICT (name_norm) DO UPDATE SET name=EXCLUDED.name "
        "RETURNING id",
        (data["brand"], norm(data["brand"])),
    )
    brand_id = cur.fetchone()[0]

    cur.execute(
        "INSERT INTO perfume(brand_id, name, name_norm, year, concentration, gender) "
        "VALUES (%s, %s, %s, %s, %s, %s) "
        "ON CONFLICT (brand_id, name_norm) "
        "DO UPDATE SET year=EXCLUDED.year, concentration=EXCLUDED.concentration, gender=EXCLUDED.gender "
        "RETURNING id",
        (brand_id, data["name"], norm(data["name"]), data.get("year"), data.get("concentration"), data.get("gender")),
    )
    perfume_id = cur.fetchone()[0]

    for p in data.get("perfumers", []):
        cur.execute(
            "INSERT INTO perfumer(name, name_norm) "
            "VALUES (%s, %s) "
            "ON CONFLICT (name_norm) DO UPDATE SET name=EXCLUDED.name "
            "RETURNING id",
            (p, norm(p)),
        )
        perfumer_id = cur.fetchone()[0]
        cur.execute(
            "INSERT INTO perfume_perfumer(perfume_id, perfumer_id, role) "
            "VALUES (%s, %s, %s) "
            "ON CONFLICT (perfume_id, perfumer_id) DO UPDATE SET role=EXCLUDED.role",
            (data["perfume_id"], perfumer_id, "creator"),
        )

    for n, pos in data.get("notes", []):
        cur.execute(
            "INSERT INTO note(name, name_norm) "
            "VALUES (%s, %s) "
            "ON CONFLICT (name_norm) DO UPDATE SET name=EXCLUDED.name "
            "RETURNING id",
            (n, norm(n)),
        )
        note_id = cur.fetchone()[0]
        cur.execute(
            "INSERT INTO perfume_note(perfume_id, note_id, note_position) "
            "VALUES (%s, %s, %s) "
            "ON CONFLICT (perfume_id, note_id) DO UPDATE SET note_position=EXCLUDED.note_position",
            (data["perfume_id"], note_id, pos),
        )

    cur.execute(
        "INSERT INTO perfume_source(perfume_id, source_id, url, raw_json) "
        "VALUES (%s, %s, %s, %s) "
        "ON CONFLICT (perfume_id, source_id) "
        "DO UPDATE SET url=EXCLUDED.url, raw_json=EXCLUDED.raw_json, last_seen=NOW()",
        (data["perfume_id"], source_id, data["url"], json.dumps(data, ensure_ascii=False)),
    )

    return data["perfume_id"]

def scrape_one(url: str) -> dict:
    html = http_get(url)
    soup = BeautifulSoup(html, "lxml")

    brand = parse_brand_from_url(url)
    gender, year, name_h1 = parse_gender_year_and_name(soup)
    name = name_h1 or url.rstrip("/").split("/")[-1].replace("-", " ").strip()

    notes, perfumers = parse_notes_and_perfumers(soup)

    return {
        "url": url,
        "brand": brand,
        "name": name,
        "year": year,
        "gender": gender,
        "concentration": None,
        "perfumers": perfumers,
        "notes": notes,
    }

def db_upsert_full(cur, data: dict) -> int:
    # source
    cur.execute(
        "INSERT INTO source(name, base_url, reliability) "
        "VALUES (%s, %s, 80) "
        "ON CONFLICT (name) DO UPDATE SET base_url=EXCLUDED.base_url "
        "RETURNING id",
        (SOURCE_NAME, SOURCE_BASE_URL),
    )
    source_id = cur.fetchone()[0]

    # brand
    cur.execute(
        "INSERT INTO brand(name, name_norm) "
        "VALUES (%s, %s) "
        "ON CONFLICT (name_norm) DO UPDATE SET name=EXCLUDED.name "
        "RETURNING id",
        (data["brand"], norm(data["brand"])),
    )
    brand_id = cur.fetchone()[0]

    # perfume
    cur.execute(
        "INSERT INTO perfume(brand_id, name, name_norm, year, concentration, gender) "
        "VALUES (%s, %s, %s, %s, %s, %s) "
        "ON CONFLICT (brand_id, name_norm) "
        "DO UPDATE SET year=EXCLUDED.year, concentration=EXCLUDED.concentration, gender=EXCLUDED.gender "
        "RETURNING id",
        (brand_id, data["name"], norm(data["name"]), data.get("year"), data.get("concentration"), data.get("gender")),
    )
    perfume_id = cur.fetchone()[0]

    # perfumers
    for p in data.get("perfumers", []):
        cur.execute(
            "INSERT INTO perfumer(name, name_norm) "
            "VALUES (%s, %s) "
            "ON CONFLICT (name_norm) DO UPDATE SET name=EXCLUDED.name "
            "RETURNING id",
            (p, norm(p)),
        )
        perfumer_id = cur.fetchone()[0]
        cur.execute(
            "INSERT INTO perfume_perfumer(perfume_id, perfumer_id, role) "
            "VALUES (%s, %s, %s) "
            "ON CONFLICT (perfume_id, perfumer_id) DO UPDATE SET role=EXCLUDED.role",
            (perfume_id, perfumer_id, "creator"),
        )

    # notes
    for n, pos in data.get("notes", []):
        cur.execute(
            "INSERT INTO note(name, name_norm) "
            "VALUES (%s, %s) "
            "ON CONFLICT (name_norm) DO UPDATE SET name=EXCLUDED.name "
            "RETURNING id",
            (n, norm(n)),
        )
        note_id = cur.fetchone()[0]
        cur.execute(
            "INSERT INTO perfume_note(perfume_id, note_id, note_position) "
            "VALUES (%s, %s, %s) "
            "ON CONFLICT (perfume_id, note_id) DO UPDATE SET note_position=EXCLUDED.note_position",
            (perfume_id, note_id, pos),
        )

    # perfume_source
    cur.execute(
        "INSERT INTO perfume_source(perfume_id, source_id, url, raw_json) "
        "VALUES (%s, %s, %s, %s) "
        "ON CONFLICT (perfume_id, source_id) "
        "DO UPDATE SET url=EXCLUDED.url, raw_json=EXCLUDED.raw_json, last_seen=NOW()",
        (perfume_id, source_id, data["url"], json.dumps(data, ensure_ascii=False)),
    )

    return perfume_id

def main():
    all_urls: list[str] = []
    seen: set[str] = set()

    print(f"START_URL = {START_URL}")
    print(f"MAX_PAGES = {MAX_PAGES} | LIMIT = {LIMIT}")

    for p in range(1, MAX_PAGES + 1):
        u = page_url(START_URL, p)
        html = http_get(u)
        batch = extract_perfume_urls_from_listing(html, limit=LIMIT)

        new = [x for x in batch if x not in seen]
        for x in new:
            seen.add(x)
            all_urls.append(x)

        print(f"Page {p}: encontradas {len(batch)} (nuevas {len(new)}). Total: {len(all_urls)}")

        if len(new) == 0:
            break
        if len(all_urls) >= LIMIT:
            all_urls = all_urls[:LIMIT]
            break

    if not all_urls:
        print("AVISO: 0 URLs totales. Cambia START_URL a un listado Top que funcione.")
        print("DONE.")
        return

    with psycopg2.connect(DB_URL) as conn:
        with conn.cursor() as cur:
            ok = 0
            for url in all_urls:
                try:
                    data = scrape_one(url)
                    pid = db_upsert_full(cur, data)
                    ok += 1
                    print(f"OK [{ok}/{len(all_urls)}] perfume_id={pid} <- {url}")
                except Exception as e:
                    print(f"ERROR en {url}: {e}")
            conn.commit()

    print("DONE.")

if __name__ == "__main__":
    main()
