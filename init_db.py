import os
import psycopg2

DB_URL = os.environ["DATABASE_URL"]

def main():
    with psycopg2.connect(DB_URL) as conn:
        with conn.cursor() as cur:
            with open("schema.sql", "r", encoding="utf-8") as f:
                cur.execute(f.read())
        conn.commit()
    print("OK: schema aplicado correctamente")

if __name__ == "__main__":
    main()
