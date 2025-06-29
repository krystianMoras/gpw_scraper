import os
from datetime import datetime, timedelta
import asyncio

import pandas as pd
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, Page as AsyncPage
from pydantic import BaseModel, HttpUrl, Field
import sqlite3

# === Constants ===
BASE_URL = "https://www.gpw.pl"
URLS_FILE = "data/company_urls.csv"
OUTPUT_DIR = "data"
LOG_FILE = os.path.join(OUTPUT_DIR, "scrape_log.csv")
SQLITE_DB = os.path.join(OUTPUT_DIR, "gpw_data.sqlite")

TABS = {
    "info": "#infoTab",
    "quotations": "#quotationsTab",
    "indicators": "#indicatorsTab",
    "reports1": "#reportsTab1",
    "reports2": "#reportsTab2",
    "shareholders": "#shareholdersTab",
    "notoria": "#showNotoria",
    # "onp": "#onpTab",
}

# Ensure output directory
os.makedirs(OUTPUT_DIR, exist_ok=True)

# === Data Models ===
class CompanyData(BaseModel):
    url: HttpUrl
    isin: str
    name: str | None = None
    description: str | None = None
    ticker: str | None = None
    full_name: str | None = None
    president: str | None = None
    province: str | None = None
    address: str | None = None
    phone: str | None = None
    fax: str | None = None
    website: str | None = None
    email: str | None = None
    debut_date: str | None = None
    issued_shares: str | None = None
    market_cap_mln: str | None = None
    book_value_mln: str | None = None
    price_to_book: str | None = None
    pe_ratio: str | None = None
    dividend_yield_percent: str | None = None
    index_membership: list[str] = []
    last_price: str | None = None
    change: str | None = None
    bid: str | None = None
    ask: str | None = None
    min_price: str | None = None
    max_price: str | None = None
    volume: str | None = None
    turnover_value: str | None = None
    debut_price: str | None = None
    max_52w: str | None = None
    min_52w: str | None = None
    market_segment: str | None = None
    sector: str | None = None

class Report(BaseModel):
    company_url: HttpUrl
    tab: str = Field(..., description="reportsTab1 or reportsTab2")
    date: str
    report_url: HttpUrl

class Shareholder(BaseModel):
    company_url: HttpUrl
    shareholder: str
    shares_count: str
    shares_pct: str
    votes_count: str
    votes_pct: str

class NotoriaMetric(BaseModel):
    company_url: HttpUrl
    metric: str
    value: str

# class OnpEvent(BaseModel):
#     company_url: HttpUrl
#     last_day_with_right: str
#     operation_type: str
#     parameter: str

# === Parsing Helpers ===

def get_text_from(soup: BeautifulSoup, selector: str, label_contains: str) -> str | None:
    row = soup.select_one(f"{selector} tr:has(th:-soup-contains('{label_contains}')) td")
    return row.get_text(strip=True).replace("\xa0", " ") if row else None

# === Extractors ===

def parse_core(html: str, url: str) -> CompanyData:
    soup = BeautifulSoup(html, 'html.parser')
    indicators = TABS['indicators']
    info = TABS['info']
    quo = TABS['quotations']
    # index membership
    qs = soup.select(f"{quo} tr:has(th:-soup-contains('Przynależność do indeksu')) td a")
    indexes = [a.get_text(strip=True) for a in qs]
    return CompanyData(
        url=url,
        isin=get_text_from(soup, indicators, 'ISIN') or '',
        name=get_text_from(soup, info, 'Nazwa:'),
        ticker=get_text_from(soup, info, 'Skrót:'),
        full_name=get_text_from(soup, info, 'Nazwa pełna:'),
        president=get_text_from(soup, info, 'Prezes Zarządu:'),
        province=get_text_from(soup, info, 'Województwo:'),
        address=get_text_from(soup, info, 'Adres siedziby:'),
        phone=get_text_from(soup, info, 'Numer telefonu:'),
        fax=get_text_from(soup, info, 'Numer faksu:'),
        website=get_text_from(soup, info, 'Strona www:'),
        email=get_text_from(soup, info, 'E-mail:'),
        debut_date=get_text_from(soup, info, 'Na giełdzie od:'),
        issued_shares=get_text_from(soup, indicators, 'Liczba wyemitowanych akcji'),
        market_cap_mln=get_text_from(soup, indicators, 'Wartość rynkowa'),
        book_value_mln=get_text_from(soup, indicators, 'Wartość księgowa'),
        price_to_book=get_text_from(soup, indicators, 'C/WK'),
        pe_ratio=get_text_from(soup, indicators, 'C/Z'),
        dividend_yield_percent=get_text_from(soup, indicators, 'Stopa dywidendy'),
        index_membership=indexes,
        last_price=get_text_from(soup, quo, 'Kurs ostatni'),
        change=get_text_from(soup, quo, 'Zmiana'),
        bid=get_text_from(soup, quo, 'Oferta kupna'),
        ask=get_text_from(soup, quo, 'Oferta sprzedaży'),
        min_price=get_text_from(soup, quo, 'Min.'),
        max_price=get_text_from(soup, quo, 'Max.'),
        volume=get_text_from(soup, quo, 'Wol. obrotu'),
        turnover_value=get_text_from(soup, quo, 'Wart. obrotu'),
        debut_price=get_text_from(soup, quo, 'Data i kurs debiutu'),
        max_52w=get_text_from(soup, quo, 'Max historyczny'),
        min_52w=get_text_from(soup, quo, 'Min historyczny'),
        market_segment=get_text_from(soup, indicators, 'Rynek/Segment'),
        sector=get_text_from(soup, indicators, 'Sektor'),
    )


def extract_reports(soup: BeautifulSoup, tab_key: str, url: HttpUrl) -> list[Report]:
    tab = TABS[tab_key]
    rows = soup.select(f"{tab} table tbody tr")
    out = []
    for row in rows:
        date_a = row.select_one("td:nth-of-type(1) a[href^='komunikat']")
        if not date_a or date_a.text.strip() == 'Data':
            continue
        out.append(Report(
            company_url=url,  # fill below
            tab=tab_key,
            date=date_a.text.strip(),
            report_url=BASE_URL + "/" + date_a['href'],
        ))
    return out


def extract_shareholders(soup: BeautifulSoup, url: HttpUrl) -> list[Shareholder]:
    rows = soup.select(f"{TABS['shareholders']} table tbody tr")
    out = []
    for row in rows:
        if row.find('th'):
            continue
        cells = row.select('td')
        if len(cells) < 5:
            continue
        out.append(Shareholder(
            company_url=url,
            shareholder=cells[0].text.strip(),
            shares_count=cells[1].text.strip().replace('\xa0',' '),
            shares_pct=cells[2].text.strip(),
            votes_count=cells[3].text.strip().replace('\xa0',' '),
            votes_pct=cells[4].text.strip(),
        ))
    return out


def extract_notoria(soup: BeautifulSoup, url: HttpUrl) -> list[NotoriaMetric]:
    rows = soup.select(f"{TABS['notoria']} table tbody tr")
    out = []
    for row in rows:
        th = row.find('th')
        td = row.find('td')
        if not th or not td:
            continue
        out.append(NotoriaMetric(
            company_url=url,
            metric=th.text.strip(),
            value=td.text.strip().replace('\xa0',' '),
        ))
    return out


# def extract_onp(soup: BeautifulSoup, url: HttpUrl) -> list[OnpEvent]:
#     rows = soup.select(f"{TABS['onp']} table tbody tr")
#     out = []
#     for row in rows:
#         if row.find('th'):
#             continue
#         cells = row.select('td')
#         if len(cells) < 3:
#             continue
#         out.append(OnpEvent(
#             company_url=url,
#             last_day_with_right=cells[0].text.strip(),
#             operation_type=cells[1].text.strip(),
#             parameter=cells[2].text.strip(),
#         ))
#     return out


def load_scrape_log() -> dict[str, datetime]:
    if not os.path.exists(LOG_FILE):
        return {}
    df = pd.read_csv(LOG_FILE, parse_dates=["last_scraped"] )
    return {row["url"]: row["last_scraped"] for _, row in df.iterrows()}

def save_scrape_log(log: dict[str, datetime]):
    filepath = LOG_FILE
    # Load existing log if exists
    if os.path.exists(filepath):
        df_existing = pd.read_csv(filepath, parse_dates=["last_scraped"])
    else:
        df_existing = pd.DataFrame(columns=["url", "last_scraped"])
    
    # Convert the new log dict to DataFrame
    df_new = pd.DataFrame([{"url": k, "last_scraped": v} for k, v in log.items()])
    
    # Remove existing entries for urls in new log
    urls_to_update = df_new["url"].tolist()
    df_existing = df_existing[~df_existing["url"].isin(urls_to_update)]
    
    # Combine old entries with new entries
    df_updated = pd.concat([df_existing, df_new], ignore_index=True)
    
    # Save full updated log, overwriting file
    df_updated.to_csv(filepath, index=False)



def update_csv(filename: str, rows: list[dict], url_key='url'):
    if not rows:
        return
    filepath = os.path.join(OUTPUT_DIR, filename)
    
    # Load existing data if file exists
    if os.path.exists(filepath):
        df_existing = pd.read_csv(filepath)
    else:
        df_existing = pd.DataFrame()
    
    # Convert new data to DataFrame
    df_new = pd.DataFrame(rows)
    
    # For your data models, company URL might have different keys like 'url', 'company_url' etc.
    # Ensure to unify column for merging/updating. Let's generalize:
    if url_key not in df_new.columns:
        # Try to find a column with 'url' in the name
        possible_url_cols = [col for col in df_new.columns if 'url' in col]
        if possible_url_cols:
            url_key = possible_url_cols[0]
    
    if url_key in df_existing.columns:
        # Remove rows with the same url from existing
        df_existing = df_existing[df_existing[url_key] != rows[0][url_key]]
        # Append new rows
        df_updated = pd.concat([df_existing, df_new], ignore_index=True)
    else:
        df_updated = df_new
    
    df_updated.to_csv(filepath, index=False)

def get_sqlite_conn():
    return sqlite3.connect(SQLITE_DB)

def update_sqlite(table: str, rows: list[dict], url_key='url'):
    if not rows:
        return
    conn = get_sqlite_conn()
    df_new = pd.DataFrame(rows)
    # Remove existing rows with the same url/company_url
    if url_key not in df_new.columns:
        possible_url_cols = [col for col in df_new.columns if 'url' in col]
        if possible_url_cols:
            url_key = possible_url_cols[0]
    try:
        df_existing = pd.read_sql(f'SELECT * FROM {table}', conn)
        if url_key in df_existing.columns:
            df_existing = df_existing[df_existing[url_key] != rows[0][url_key]]
            df_updated = pd.concat([df_existing, df_new], ignore_index=True)
        else:
            df_updated = df_new
    except Exception:
        # Table does not exist yet
        df_updated = df_new
    df_updated.to_sql(table, conn, if_exists='replace', index=False)
    conn.close()


def process_company_data(url: str, tab_html: dict, description: str | None):
    """Extract all company data and update SQLite tables."""
    def to_serializable(d):
        # Recursively convert HttpUrl and other non-serializable types to str
        if isinstance(d, dict):
            return {k: to_serializable(v) for k, v in d.items()}
        elif isinstance(d, list):
            # If it's a list of strings, join with comma
            if all(isinstance(x, str) for x in d):
                return ','.join(d)
            else:
                return [to_serializable(x) for x in d]
        elif isinstance(d, (HttpUrl,)):
            return str(d)
        return d

    company_data = []
    reports_data = []
    shareholders_data = []
    notoria_data = []
    # onp_data = []
    merged_html = tab_html['info'] + tab_html['indicators'] + tab_html['quotations']
    cd = parse_core(merged_html, url)
    cd.description = description
    company_data.append(to_serializable(cd.model_dump()))
    for key in ('reports1', 'reports2'):
        soup = BeautifulSoup(tab_html[key], 'html.parser')
        reps = extract_reports(soup, key, url)
        for r in reps:
            reports_data.append(to_serializable(r.model_dump()))
    soup = BeautifulSoup(tab_html['shareholders'], 'html.parser')
    shs = extract_shareholders(soup, url)
    for sh in shs:
        shareholders_data.append(to_serializable(sh.model_dump()))
    soup = BeautifulSoup(tab_html['notoria'], 'html.parser')
    nts = extract_notoria(soup, url)
    for n in nts:
        notoria_data.append(to_serializable(n.model_dump()))
    # soup = BeautifulSoup(tab_html['onp'], 'html.parser')
    # onp_events = extract_onp(soup, url)
    # for e in onp_events:
    #     onp_data.append(to_serializable(e.model_dump()))
    
    print(f"✅ Processed {url} - Company: {cd.name}, Description: {cd.description[:30]}...")
    update_sqlite("company_company", company_data, url_key='url')
    update_sqlite("company_reports", reports_data, url_key='company_url')
    update_sqlite("company_shareholders", shareholders_data, url_key='company_url')
    update_sqlite("company_notoria", notoria_data, url_key='company_url')
    # update_sqlite("company_onp", onp_data, url_key='company_url')

BATCH_SIZE = 1 # Number of pages to open in parallel

async def extract_description_async(page: AsyncPage) -> str | None:

    return (await page.locator("div.comapny-description > div:nth-child(2)").inner_text(timeout=3000)).strip()


async def collect_tab_html_async(page: AsyncPage) -> dict:
    tab_html = {'info': await page.content()}
    tab_selectors = {
        'indicators': '#indicatorsTab table',
        'quotations': '#quotationsTab table',
        'reports1': '#reportsTab1 table',
        'reports2': '#reportsTab2 table',
        'shareholders': '#shareholdersTab table',
        'notoria': '#showNotoria table',
        # 'onp': '#onpTab table',
    }
    for tab_key in ['indicators', 'quotations', 'reports1', 'reports2', 'shareholders', 'notoria']:
        selector = f'a.nav-link[href="{TABS[tab_key]}"]'
        try:
            await page.click(selector, timeout=15000)
            await page.wait_for_selector(tab_selectors[tab_key], timeout=5000)
            tab_html[tab_key] = await page.content()
        except Exception as e:
            print(f"⚠️ Could not click or wait for {tab_key}: {e}")
            tab_html[tab_key] = await page.content()
    return tab_html

async def scrape_url_async(context, url, scrape_log, skip_threshold, semaphore):
    async with semaphore:
        last = scrape_log.get(url)
        if last and last > skip_threshold:
            print(f"⏭ Skipping (recently scraped): {url}")
            return url, False
        print(f"🔍 Scraping: {url}")
        page: AsyncPage = await context.new_page()
        try:
            await page.goto(url, timeout=30000)
            description = await extract_description_async(page)
            tab_html = await collect_tab_html_async(page)
            process_company_data(url, tab_html, description)
            scrape_log[url] = datetime.now()
            save_scrape_log(scrape_log)
            return url, True
        except Exception as e:
            print(f"⚠️ Error for {url}: {e}")
            return url, False
        finally:
            await page.close()

async def scrape_all_async():
    urls = pd.read_csv(URLS_FILE, header=None, names=['url'])['url'].tolist()
    scrape_log = load_scrape_log()
    now = datetime.now()
    skip_threshold = now - timedelta(days=1)
    semaphore = asyncio.Semaphore(BATCH_SIZE)
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        tasks = [scrape_url_async(context, url, scrape_log, skip_threshold, semaphore) for url in urls]
        await asyncio.gather(*tasks)
        await browser.close()



if __name__ == '__main__':
    asyncio.run(scrape_all_async())