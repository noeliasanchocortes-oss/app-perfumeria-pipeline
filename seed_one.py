import os
import json
import re
import psycopg2

DB_URL = os.environ["DATABASE_URL"]

def norm(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s

def upsert_one(cur):
    # Ejemplo controlado (luego lo sustituimos por scraping)
    source_name = "manual_seed"
    source_url = "https://example.com/perfume/seed"

    brand_name = "Dior"
    perfume_name = "Sauvage"
    year = 2015
    concentration = "EDT"
    gender = "male"

    perfumers = ["François Demachy"]
    notes = [("Bergamot", "top"), ("Pepper", "top"), ("Ambroxan", "base")]

    raw = {
        "brand": brand_name,
        "name": perfume_name,
        "year": year,
        "concentration": concentration,
        "gender": gender,
        "perfumers": perfumers,
        "notes": [{"name": n, "pos": p} for n, p in notes],
        "url": source_url,
        "source": source_name,
    }

    # source
    cur.execute(
        """
        INSERT INTO source(name, base_url, reliability)
        VALUES (%s, %s, 10)
        ON CONFLICT (name) DO UPDATE SET base_url=EXCLUDED.base_url
        RETURNING id
        """,
        (source_name, "https://example.com"),
    )
    source_id = cur.fetchone()[0]

    # brand
    cur.execute(
        """
        INSERT INTO brand(name, name_norm)
        VALUES (%s, %s)
        ON CONFLICT (name_norm) DO UPDATE SET name=EXCLUDED.name
        RETURNING id
        """,
        (brand_name, norm(brand_name)),
    )
    brand_id = cur.fetchone()[0]

    # perfume
    cur.execute(
        """
        INSERT INTO perfume(brand_id, name, name_norm, year, concentration, gender)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (brand_id, name_norm)
        DO UPDATE SET year=EXCLUDED.year, concentration=EXCLUDED.concentration, gender=EXCLUDED.gender
        RETURNING id
        """,
        (brand_id, perfume_name, norm(perfume_name), year, concentration, gender),
    )
    perfume_id = cur.fetchone()[0]

    # perfumers + link
    for p in perfumers:
        cur.execute(
            """
            INSERT INTO perfumer(name, name_norm)
            VALUES (%s, %s)
            ON CONFLICT (name_norm) DO UPDATE SET name=EXCLUDED.name
            RETURNING id
            """,
            (p, norm(p)),
        )
        perfumer_id = cur.fetchone()[0]

        cur.execute(
            """
            INSERT INTO perfume_perfumer(perfume_id, perfumer_id, role)
            VALUES (%s, %s, %s)
            ON CONFLICT (perfume_id, perfumer_id) DO UPDATE SET role=EXCLUDED.role
            """,
            (perfume_id, perfumer_id, "creator"),
        )

    # notes + link
    for n, pos in notes:
        cur.execute(
            """
            INSERT INTO note(name, name_norm)
            VALUES (%s, %s)
            ON CONFLICT (name_norm) DO UPDATE SET name=EXCLUDED.name
            RETURNING id
            """,
            (n, norm(n)),
        )
        note_id = cur.fetchone()[0]

        cur.execute(
            """
            INSERT INTO perfume_note(perfume_id, note_id, note_position)
            VALUES (%s, %s, %s)
            ON CONFLICT (perfume_id, note_id) DO UPDATE SET note_position=EXCLUDED.note_position
            """,
            (perfume_id, note_id, pos),
        )

    # perfume_source (raw_json + tracking)
    cur.execute(
        """
        INSERT INTO perfume_source(perfume_id, source_id, url, raw_json)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (perfume_id, source_id)
        DO UPDATE SET url=EXCLUDED.url, raw_json=EXCLUDED.raw_json, last_seen=NOW()
