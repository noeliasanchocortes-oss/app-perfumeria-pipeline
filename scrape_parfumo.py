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

UA = "NuvioPerfumeriaBot/0.1 (respectful; contact: your-email@example.com)"

def norm(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s

def http_get(url: str) -> str:
    # Pequeño delay para no machacar el servidor
    time.sleep(1.0 + random.random() * 0.5)
    r = requests.get(url, headers={"User-Agent": UA}, timeout=30)
    r.raise_for_status()
    return r.text

def extract_perfume_urls_from_index(html: str, limit: int) -> list[str]:
    soup = BeautifulSoup(html, "lxml")
    urls = []
    seen = set()

    # Captura enlaces que apunten a páginas de perfume:
    # /Perfumes/Brand/slug  o /Parfums/Brand/slug
    for a in soup.select("a[href]"):
        href = a.get("href", "")
        if not href:
            continue
        if href.startswith("/Perfumes/") or href.startswith("/Parfums/"):
            # Debe tener al menos 3 segmentos: /Perfumes/Brand/slug
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
    # En Parfumo suele haber una frase cerca del encabezado:
    # "A perfume by BRAND for women and men, released in 2023."
    text = soup.get_text("\n", strip=True)

    # year
    year = None
    m = re.search(r"released in\s+(\d{4})", text, re.IGNORECASE)
    if m:
        year = int(m.group(1))
    else:
        # fallback: primer año 19xx/20xx cerca del top (no perfecto, pero útil)
        m2 = re.search(r"\b(19|20)\d{2}\b", text)
        if m2:
            year = int(m2.group(0))

    # gender
    gender = None
    # orden importante
    if re.search(r"for women and men|for men and women", text, re.IGNORECASE):
        gender = "unisex"
    elif re.search(r"for men\b", text, re.IGNORECASE):
        gender = "male"
    elif re.search(r"for women\b", text, re.IGNORECASE):
        gender = "female"

    # name (mejor desde h1)
    name = None
    h1 = soup.select_one("h1")
    if h1:
        name = h1.get_text(" ", strip=True)

    return gender, year, name

def parse_brand_from_url(url: str) -> str:
    # https://www.parfumo.com/Perfumes/Jusbox/carioca-heart
    parts = url.split("/")
    try:
        idx = parts.index("Perfumes")
    except ValueError:
        idx = parts.index("Parfums")
    brand_slug = parts[idx + 1]
    return brand_slug.replace("_", " ").strip()

def parse_notes_and_perfumers(soup: BeautifulSoup) -> tuple[list[tuple[str, str]], list[str]]:
    """
    Returns:
      notes: list of (note_name, position) where position in {top, heart, base}
      perfumers: list[str]
    """
    text = soup.get_text("\n", strip=True)

    # Perfumers: Parfumo suele mostrar un bloque "Perfumer" con enlaces justo debajo. :contentReference[oaicite:2]{index=2}
    perfumers = []
    # estrategia simple: busca la línea exacta "Perfumer" y captura las siguientes líneas hasta "Ratings"
    lines = text.split("\n")
    for i, line in enumerate(lines):
        if line.strip().lower() == "perfumer":
            j = i + 1
            while j < len(lines):
                nxt = lines[j].strip()
                if not nxt or nxt.lower() in {"ratings", "rating"}:
                    break
                # evita basura típica
                if len(nxt) <= 60 and "ratings" not in nxt.lower():
                    perfumers.append(nxt)
                j += 1
            break

    # Notes: Parfumo muestra "Fragrance Pyramid" y luego Top/Heart/Base con nombres. :contentReference[oaicite:3]{index=3}
    notes = []
    def collect_after(label: str, pos: str):
        try:
            idx = lines.index(label)
        except ValueError:
            return
        j = idx + 1
        # salta encabezados tipo "Top Notes"
        while j < len(lines):
            nxt = lines[j].strip()
            if not nxt:
                j += 1
                continue
            low = nxt.lower()
            if low in {"heart notes", "base notes", "top notes", "perfumer", "ratings"}:
                break
            # filtra líneas que sean claramente notas (cortas)
            if len(nxt) <= 40 and not nxt.lower().startswith("image:"):
                notes.append((nxt, pos))
            j += 1

    # Si existe la pirámide, suele contener estos labels
    if "Fragrance Pyramid" in lines:
        # aseguramos que haya secciones
        if "Top Notes" in lines:
            collect_after("Top Notes", "top")
        if "Heart Notes" in lines:
            collect_after("Heart Notes", "heart")
        if "Base Notes" in lines:
            collect_after("Base Notes", "base")

    # dedupe perfumers y notes
    perfumers = list(dict.fromkeys([p.strip() for p in perfumers if p.strip()]))
    seen_notes = set()
    clean_notes = []
    for n, pos in notes:
        key = (norm(n), pos)
        if key not in seen_notes:
            seen_notes.add(key)
            clean_notes.append((n, pos))
    return clean_notes, perfumers

def db_upsert(cur, data: dict) -> int:
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

    # perfume_source (raw_json + tracking)
    cur.execute(
        "INSERT INTO perfume_source(perfume_id, source_id, url, raw_json) "
        "VALUES (%s, %s, %s, %s) "
        "ON CONFLICT (perfume_id, source_id) "
        "DO UPDATE SET url=EXCLUDED.url, raw_json=EXCLUDED.raw_json, last_seen=NOW()",
        (perfume_id, source_id, data["url"], json.dumps(data, ensure_ascii=False)),
    )

    return perfume_id

def scrape_one(url: str) -> dict:
    html = http_get(url)
    soup = BeautifulSoup(html, "lxml")

    brand = parse_brand_from_url(url)
    gender, year, name_h1 = parse_gender_year_and_name(soup)

    # El h1 puede incluir brand/año; preferimos nombre limpio desde URL si está raro
    # pero normalmente el h1 es usable.
    name = name_h1 or url.rstrip("/").split("/")[-1].replace("-", " ").title()

    notes, perfumers = parse_notes_and_perfumers(soup)

    return {
        "source": SOURCE_NAME,
        "url": url,
        "brand": brand,
        "name": name,
        "year": year,
        "gender": gender,
        "concentration": None,  # lo añadiremos en la siguiente iteración si aparece en la ficha
        "perfumers": perfumers,
        "notes": notes,  # [(name, "top"/"heart"/"base")]
    }

def main():
    limit = int(os.environ.get("LIMIT", "10"))  # por defecto 10 para no meter carga
    index_html = http_get(START_INDEX)
    urls = extract_perfume_urls_from_index(index_html, limit)

    print(f"Index OK. Encontradas {len(urls)} URLs.")

    with psycopg2.connect(DB_URL) as conn:
        with conn.cursor() as cur:
            ok = 0
            for u in urls:
                try:
                    data = scrape_one(u)
                    pid = db_upsert(cur, data)
                    ok += 1
                    print(f"OK [{ok}/{len(urls)}] perfume_id={pid} <- {u}")
                except Exception as e:
                    print(f"ERROR en {u}: {e}")
            conn.commit()

    print("DONE.")

if __name__ == "__main__":
    main()
