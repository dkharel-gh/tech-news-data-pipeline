import json
import os
import sys
import time
import urllib.request


def fetch_json(url, timeout=10):
    req = urllib.request.Request(url, headers={"User-Agent": "etl-test"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.load(resp)


def main():
    base_url = os.getenv("HN_BASE_URL", "https://hacker-news.firebaseio.com/v0").rstrip("/")
    top_n = int(os.getenv("HN_TOP_STORIES", "10"))
    timeout = int(os.getenv("HTTP_TIMEOUT_SECONDS", "10"))

    print(f"Base URL: {base_url}")
    print(f"Top N: {top_n}")

    ids = fetch_json(f"{base_url}/topstories.json", timeout=timeout)[:top_n]
    print(f"Fetched {len(ids)} story ids")

    for i, story_id in enumerate(ids, start=1):
        item = fetch_json(f"{base_url}/item/{story_id}.json", timeout=timeout)
        title = item.get("title", "<no title>")
        url = item.get("url", "<no url>")
        score = item.get("score", 0)
        by = item.get("by", "<unknown>")
        ts = item.get("time", 0)
        print(f"{i:02d}. [{score}] {title}")
        print(f"    by {by} at {time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(ts))} UTC")
        print(f"    {url}")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise
