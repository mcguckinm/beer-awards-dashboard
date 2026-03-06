from __future__ import annotations
import time
import re
import requests
import pandas as pd
from pathlib import Path
from bs4 import BeautifulSoup
from pypdf import PdfReader
import pdfplumber
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

from utils import DATA_RAW, DATA_CLEAN, utc_now_iso, load_settings

USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome Safari"

def get_driver(headless: bool) -> webdriver.Chrome:
    opts = webdriver.ChromeOptions()
    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument("--window-size=1400,900")
    opts.add_argument(f"user-agent={USER_AGENT}")
    opts.add_argument("--disable-gpu")
    return webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)

def polite_sleep(seconds: float) -> None:
    time.sleep(seconds)

def safe_text(el) -> str | None:
    return el.get_text(" ", strip=True) if el else None

def parse_range(text: str | None) -> tuple[float | None, float | None]:
    if not text:
        return None, None

    nums = re.findall(r"(\d+(?:\.\d+)?)", text)
    if len(nums) >= 2:
        return float(nums[0]), float(nums[1])
    if len(nums) == 1:
        v = float(nums[0])
        return v, v
    return None, None

#World beer classic winners scrape

def collect_wbc_awards(driver: webdriver.Chrome, start_url: str, max_year_pages: int, delay: float) -> pd.DataFrame:
    """
    World Beer Cup winners:
    - Use Selenium only to load the start page HTML
    - Collect PDF links
    - Download PDFs
    - Extract TEXT with pdfplumber
    - Parse lines like:
        Category: X ... Entries
        Gold: ...
        Silver: ...
        Bronze: ...
    """
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})

    driver.get(start_url)
    polite_sleep(delay)

    soup = BeautifulSoup(driver.page_source, "lxml")

    # Collect PDF links
    pdf_urls = []
    for a in soup.select("a[href]"):
        href = (a.get("href") or "").strip()
        if not href.lower().endswith(".pdf"):
            continue
        if href.startswith("/"):
            href = "https://www.worldbeercup.org" + href
        pdf_urls.append(href)

    # De-dupe + limit
    seen = set()
    pdf_urls = [u for u in pdf_urls if not (u in seen or seen.add(u))]
    pdf_urls = pdf_urls[:max_year_pages]

    print("[WBC] PDF pages:", pdf_urls)

    raw_dir = Path("data/raw")
    raw_dir.mkdir(parents=True, exist_ok=True)

    rows: list[dict] = []

    def flush_award(year: int | None, category: str | None, medal: str | None, value: str | None, source_url: str):
        if not category or not medal or not value:
            return
        v = value.strip()
        if not v:
            return
        def split_wbc_value(v: str) -> tuple[str | None, str | None, str | None]:
            # Try to split "Beer Name - Brewery Name - Country" or similar formats
            v = " ".join(v.split())  # normalize whitespace
            for sep in [" - ", " | ", " – ", " — ", " / "]:
                if sep in v:
                    left, right = v.split(sep,1)
                    beer = left.strip() or None

                    parts = [p.strip() for p in right.split(sep) if p.strip()]
                    brewery = parts[0] if parts else right.strip() or None
                    location = ", ".join(parts[1:]) if len(parts) > 1 else None
                    return beer, brewery, location
            return v.strip() or None, None, None
        beer, brewery, location = split_wbc_value(v)
        rows.append({
            "competition": "World Beer Cup",
            "year": year,
            "category": category.strip(),
            "medal": medal,
            "beer_name": beer,          # raw combined text for now
            "brewery_name": brewery,    # can split later in cleaning step
            "location": location,        # can be city, state, country, or combination
            "country": None,
            "source_url": source_url
        })

    # Patterns
    cat_re = re.compile(r"^Category\s*:?\s*(.+)$", re.IGNORECASE)
    medal_re = re.compile(r"^(Gold|Silver|Bronze)\s*:\s*(.+)$", re.IGNORECASE)

    for pdf_url in pdf_urls:
    # Prefer the competition year from the filename, not the WP upload folder
        fname = pdf_url.rsplit("/", 1)[-1]

    # Try to find a 4-digit year in the filename first (handles 2010_winners.pdf, 2008_winners.pdf, etc.)
        m = re.search(r"(19\d{2}|20\d{2})", fname)
        year = int(m.group(1)) if m else None

    # Fallback: try the full URL if filename doesn't contain the year
        if year is None:
            m2 = re.search(r"/(19\d{2}|20\d{2})_", pdf_url) or re.search(r"/(19\d{2}|20\d{2})/", pdf_url)
            year = int(m2.group(1)) if m2 else None

    # ✅ Save PDFs with a unique name so you never overwrite
        safe_stub = re.sub(r"[^A-Za-z0-9]+", "_", fname).strip("_")
        pdf_path = raw_dir / f"wbc_{safe_stub}"

        if not pdf_path.exists():
            r = session.get(pdf_url, timeout=60)
            r.raise_for_status()
            pdf_path.write_bytes(r.content)

        before = len(rows)

        current_category: str | None = None
        current_medal: str | None = None
        current_value: str | None = None

        with pdfplumber.open(str(pdf_path)) as pdf:
            for page in pdf.pages:
                text = page.extract_text() or ""
                if not text.strip():
                    continue

                lines = [ln.strip() for ln in text.splitlines() if ln.strip()]

                for line in lines:
                    cm = cat_re.match(line)
                    if cm:
                        flush_award(year, current_category, current_medal, current_value, pdf_url)
                        current_medal = None
                        current_value = None
                        current_category = cm.group(1)
                        continue

                    mm = medal_re.match(line)
                    if mm:
                        flush_award(year, current_category, current_medal, current_value, pdf_url)
                        current_medal = mm.group(1).title()
                        current_value = mm.group(2).strip()
                        continue
                    if current_medal and current_value:
                        if line.lower().startswith("category"):
                            continue
                        if line.lower().startswith(("gold:", "silver:", "bronze:")):
                            continue
                        current_value += " " + line

        flush_award(year, current_category, current_medal, current_value, pdf_url)

        added = len(rows) - before
        print(f"[WBC] Parsed {year}: +{added} rows from {pdf_path.name}")

    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.drop_duplicates()

    return df

def collect_bjcp_styles(driver: webdriver.Chrome, start_url: str, max_pages: int, delay: float) -> pd.DataFrame:
    """
    BJCP 2021 Beer styles:
    1) Start at https://www.bjcp.org/style/2021/beer/
    2) Collect category pages like /style/2021/1/standard-american-beer/
    3) Visit each category page and collect style pages like /style/2021/1/1A/american-light-lager/
    4) Visit each style page and parse vital stats.

    max_pages limits the number of style pages scraped.
    """
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})

    # --- Step 1: get beer index page and collect ALL category urls ---
    r = session.get(start_url, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "lxml")

    category_urls: list[str] = []
    for a in soup.select("a[href]"):
        href = (a.get("href") or "").strip()
        if not href:
            continue
        if href.startswith("/"):
            href = "https://www.bjcp.org" + href

        # category pages look like:
        # https://www.bjcp.org/style/2021/1/standard-american-beer/
        if re.search(r"^https://www\.bjcp\.org/style/2021/\d{1,2}/?$", href):
            category_urls.append(href.rstrip("/") + "/")

    seen = set()
    category_urls = [u for u in category_urls if not (u in seen or seen.add(u))]

    # --- Step 2: from each category page, collect ALL style urls ---
    style_urls: list[str] = []
    for cat_url in category_urls:
        polite_sleep(delay)
        rc = session.get(cat_url, timeout=30)
        if rc.status_code != 200:
            continue
        cat_soup = BeautifulSoup(rc.text, "lxml")

        for a in cat_soup.select("a[href]"):
            href = (a.get("href") or "").strip()
            if not href:
                continue
            if href.startswith("/"):
                href = "https://www.bjcp.org" + href

            # style pages look like:
            # https://www.bjcp.org/style/2021/10/10A/weissbier/
            if re.search(r"^https://www\.bjcp\.org/style/2021/\d+/\d+[A-Z]/[^/]+/?$", href):
                style_urls.append(href.rstrip("/") + "/")

    seen = set()
    style_urls = [u for u in style_urls if not (u in seen or seen.add(u))]

    # limit by max_pages (style pages)
    style_urls = style_urls[: int(max_pages)]

    print(f"[BJCP] Found {len(style_urls)} style pages (showing up to 10): {style_urls[:10]}")

    # --- Step 3: parse each style page ---
    def extract_stat_from_lines(lines: list[str], label: str) -> tuple[float | None, float | None]:
        L = label.upper()

        # 1) same-line: "IBU 8 - 12" or "IBUs: 8 – 12"
        for ln in lines:
            u = ln.strip().upper()
            if u.startswith(L):
                lo, hi = parse_range(ln)
                if lo is not None:
                    return lo, hi

        # 2) label line then next line is numbers
        for idx, ln in enumerate(lines[:-1]):
            u = ln.strip().upper().replace(":", "")
            if u == L or u == (L + "S"):
                lo, hi = parse_range(lines[idx + 1])
                if lo is not None:
                    return lo, hi

        # 3) regex fallback
        big = "\n".join(lines)
        m = re.search(
            rf"\b{label}s?\b\s*[:\-]?\s*(\d+(?:\.\d+)?)\s*(?:–|-|to)?\s*(\d+(?:\.\d+)?)?",
            big,
            re.IGNORECASE,
        )
        if m:
            a = float(m.group(1))
            b = float(m.group(2)) if m.group(2) else a
            return a, b

        return None, None

    rows: list[dict] = []
    for i, url in enumerate(style_urls, start=1):
        polite_sleep(delay)
        rs = session.get(url, timeout=30)
        if rs.status_code != 200:
            continue

        page_soup = BeautifulSoup(rs.text, "lxml")
        title = safe_text(page_soup.find("h1")) or ""

        # "1A. American Light Lager"
        m = re.match(r"^\s*(\d{1,2}[A-Z])\.\s*(.+?)\s*$", title)
        style_id = m.group(1) if m else None
        style_name = m.group(2) if m else (title or None)

        lines = page_soup.get_text("\n", strip=True).splitlines()

        ibu_lo, ibu_hi = extract_stat_from_lines(lines, "IBU")
        srm_lo, srm_hi = extract_stat_from_lines(lines, "SRM")
        abv_lo, abv_hi = extract_stat_from_lines(lines, "ABV")
        og_lo, og_hi = extract_stat_from_lines(lines, "OG")
        fg_lo, fg_hi = extract_stat_from_lines(lines, "FG")

        if abv_lo is None and ibu_lo is None and srm_lo is None:
            continue

        rows.append({
            "style_id": style_id,
            "style_name": style_name,
            "abv_low": abv_lo, "abv_high": abv_hi,
            "ibu_low": ibu_lo, "ibu_high": ibu_hi,
            "srm_low": srm_lo, "srm_high": srm_hi,
            "og_low": og_lo, "og_high": og_hi,
            "fg_low": fg_lo, "fg_high": fg_hi,
            "source_url": url,
        })

        print(f"[BJCP {i}/{len(style_urls)}] {style_id}. {style_name} -> {url}")

    df = pd.DataFrame(rows)
    if not df.empty:
        df["style_name"] = df["style_name"].astype(str).str.strip()
        df = df.dropna(subset=["style_name"]).drop_duplicates(subset=["style_id", "style_name"])

    print(f"[BJCP] Parsed {len(df)} styles from {start_url}")
    return df

def collect_obdb_breweries(api_base: str, max_pages: int, per_page: int) -> pd.DataFrame:
    rows: list[dict] = []
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})

    api_base = api_base.rstrip("/")  # important

    for page_num in range(1, int(max_pages) + 1):
        url = f"{api_base}/breweries"
        params = {"page": page_num, "per_page": int(per_page)}

        r = session.get(url, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()

        # ✅ guard: base URL or errors return dict, not list
        if isinstance(data, dict):
            raise RuntimeError(
                f"OBDB returned an object (not a list). "
                f"Check api_base. Got url={r.url} keys={list(data.keys())[:10]}"
            )

        print(f"[OBDB] page {page_num} -> {len(data)} breweries (url={r.url})")

        if not data:
            break

        for b in data:
            rows.append({
                "obdb_id": b.get("id"),
                "brewery_name": b.get("name"),
                "brewery_type": b.get("brewery_type"),
                "city": b.get("city"),
                "state": b.get("state_province") or b.get("state"),  # new field + legacy
                "country": b.get("country"),
                "latitude": b.get("latitude"),
                "longitude": b.get("longitude"),
                "website_url": b.get("website_url"),
            })

        if len(data) < per_page:
            break

    df = pd.DataFrame(rows)
    if not df.empty:
        df["brewery_name"] = df["brewery_name"].astype(str).str.strip()
    # keep all unique records (don’t collapse multi-location breweries)
        df = df.dropna(subset=["obdb_id"]).drop_duplicates(subset=["obdb_id"])
    return df
    

def main():
    settings = load_settings()
    scraped_at = utc_now_iso()
    headless = bool(settings.get("headless", True))
    delay = float(settings.get("polite_delay_seconds", 0.4))

    driver = get_driver(headless=headless)

    wbc = collect_wbc_awards(
    driver,
    start_url=settings["wbc_start_url"],
    max_year_pages=int(settings.get("max_wbc_year_pages", 16)),
    delay=delay
    )

    wbc.insert(0, "scraped_at", scraped_at)
    wbc_path = DATA_RAW / f"wbc_awards_{scraped_at.replace(':','-')}.csv"
    wbc.to_csv(wbc_path,index=False)
    print(f"Saved WBC awards -> {wbc_path} ({len(wbc)} rows)")

    bjcp = collect_bjcp_styles(
    driver,
    start_url=settings["bjcp_start_url"],  # set this to https://www.bjcp.org/style/2021/beer/
    max_pages=int(settings.get("max_bjcp_pages", 200)),  # not really needed for BJCP since all styles are on one page, but keeping in signature for compatibility
    delay=delay
    )

    bjcp.insert(0, "scraped_at", scraped_at)
    bjcp_path = DATA_RAW / f"bjcp_styles_{scraped_at.replace(':','-')}.csv"

    bjcp.to_csv(bjcp_path, index=False)
    print(f"Saved BJCP Styles -> {bjcp_path} ({len(bjcp)} rows)")

    driver.quit()

    obdb = collect_obdb_breweries(
        api_base=settings["obdb_api_base"],
        max_pages=int(settings.get("obdb_max_pages", 500)),
        per_page=int(settings.get("obdb_per_page", 100))
    )

    obdb.insert(0, "scraped_at", scraped_at)
    obdb_path = DATA_RAW / f"obdb_breweries_{scraped_at.replace(':','-')}.csv"
    obdb.to_csv(obdb_path, index=False)
    print(f"Saved OBDB breweries -> {obdb_path} ({len(obdb)} rows)")

    DATA_CLEAN.mkdir(parents=True, exist_ok=True)
    wbc.to_csv(DATA_CLEAN / "wbc_awards_clean.csv", index=False)
    bjcp.to_csv(DATA_CLEAN / "bjcp_styles_clean.csv", index=False)
    obdb.to_csv(DATA_CLEAN / "obdb_breweries_clean.csv", index=False)

if __name__ =="__main__":
    main()