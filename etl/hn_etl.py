import json
import os
import time
import urllib.request

import psycopg2


def fetch_json(url, timeout=10):
    req = urllib.request.Request(url, headers={"User-Agent": "etl-runner"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.load(resp)


def get_db_conn():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "db"),
        port=int(os.getenv("DB_PORT", "5432")),
        dbname=os.getenv("DB_NAME", "etl"),
        user=os.getenv("DB_USER", "etl"),
        password=os.getenv("DB_PASSWORD", "etl"),
    )


def ensure_schema(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS hn_stories (
                id BIGINT PRIMARY KEY,
                title TEXT,
                url TEXT,
                score INTEGER,
                by_user TEXT,
                time_utc TIMESTAMPTZ,
                raw JSONB,
                fetched_at TIMESTAMPTZ DEFAULT NOW()
            )
            """
        )
    conn.commit()


def upsert_story(conn, item):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO hn_stories (id, title, url, score, by_user, time_utc, raw)
            VALUES (%s, %s, %s, %s, %s, to_timestamp(%s), %s::jsonb)
            ON CONFLICT (id) DO UPDATE SET
                title = EXCLUDED.title,
                url = EXCLUDED.url,
                score = EXCLUDED.score,
                by_user = EXCLUDED.by_user,
                time_utc = EXCLUDED.time_utc,
                raw = EXCLUDED.raw,
                fetched_at = NOW()
            """,
            (
                item.get("id"),
                item.get("title"),
                item.get("url"),
                item.get("score", 0),
                item.get("by"),
                item.get("time", 0),
                json.dumps(item),
            ),
        )
    conn.commit()


def main():
    base_url = os.getenv("HN_BASE_URL", "https://hacker-news.firebaseio.com/v0").rstrip(
        "/"
    )
    top_n = int(os.getenv("HN_TOP_STORIES", "50"))
    timeout = int(os.getenv("HTTP_TIMEOUT_SECONDS", "10"))

    print(f"Base URL: {base_url}")
    print(f"Top N: {top_n}")

    ids = fetch_json(f"{base_url}/topstories.json", timeout=timeout)[:top_n]
    print(f"Fetched {len(ids)} story ids")

    with get_db_conn() as conn:
        ensure_schema(conn)
        for i, story_id in enumerate(ids, start=1):
            item = fetch_json(f"{base_url}/item/{story_id}.json", timeout=timeout)
            upsert_story(conn, item)
            title = item.get("title", "<no title>")
            score = item.get("score", 0)
            by = item.get("by", "<unknown>")
            ts = item.get("time", 0)
            print(
                f"{i:02d}. [{score}] {title} by {by} at "
                f"{time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(ts))} UTC"
            )


if __name__ == "__main__":
    main()
