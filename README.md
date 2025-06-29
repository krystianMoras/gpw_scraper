# GPW Scraper

## Quickstart

1. **Fetch company URLs:**
   ```sh
   uv run fetch_company_urls.py
   ```
2. **Fetch company data:**
   ```sh
   uv run fetch_company_data.py
   ```

---

## Data Collected

Basic info is fetched from `https://www.gpw.pl/spolka?isin=<ISIN>` for each company, including:

- Company profile: name, ticker, ISIN, full name, description, president, address, contact details, sector, and market segment
- Financials: market capitalization, book value, price-to-book, P/E ratio, dividend yield, last price, price change, bid/ask, min/max prices, volume, turnover, debut price, 52-week high/low
- Index membership: list of indices the company belongs to
- Reports: links and dates for two types of company reports
- Shareholders: list of shareholders with share/vote counts and percentages
- Notoria metrics: additional company metrics and values

All data is stored in structured tables and can be accessed via the MCP server tools.


## MCP Server

MCP config:

```json
"GPW": {
  "command": "uv",
  "args": ["--project", "<path_to_repo>", "run", "<path_to_repo>/mcp_server.py"]
}
```


# osint analysis (soon)