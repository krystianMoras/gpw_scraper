import os
import pandas as pd
from typing import Set
from playwright.sync_api import sync_playwright

BASE_URL = "https://www.gpw.pl"
OUTPUT_FILE = "data/company_urls.csv"
TABLE_ROWS_SELECTOR = "tbody#search-result tr"
SHOW_MORE_SELECTOR = 'a.more[data-type="pager"]'


def load_existing_urls() -> Set[str]:
    if not os.path.exists(OUTPUT_FILE):
        return set()
    df = pd.read_csv(OUTPUT_FILE)
    return set(df['url'].dropna().unique())


def save_urls(urls: Set[str]):
    df = pd.DataFrame(sorted(urls), columns=["url"])
    df.to_csv(OUTPUT_FILE, index=False)


def fetch_company_urls():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context()
        page = context.new_page()

        page.goto(f"{BASE_URL}/spolki")

        try:
            page.get_by_role("button", name="Akceptuj wszystkie").click(timeout=3000)
        except:
            pass

        page.locator('span.pointer[onclick="charFilter(\'\')"]').click()

        while True:
            load_more = page.locator(SHOW_MORE_SELECTOR)
            if not load_more.is_visible():
                break
            rows_before = page.locator(TABLE_ROWS_SELECTOR).count()
            load_more.click()
            try:
                page.wait_for_function(
                    f"() => document.querySelectorAll('{TABLE_ROWS_SELECTOR}').length > {rows_before}",
                    timeout=5000
                )
            except:
                break

        all_links = page.locator("a").all()
        new_urls = {
            f"{BASE_URL}/{href}"
            for link in all_links
            if (href := link.get_attribute("href")) and "spolka?isin=" in href
        }

        existing_urls = load_existing_urls()
        all_urls = existing_urls.union(new_urls)
        save_urls(all_urls)

        print(f"✅ Fetched {len(new_urls)} new URLs, total: {len(all_urls)}")

        context.close()
        browser.close()


if __name__ == "__main__":
    fetch_company_urls()
