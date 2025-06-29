from mcp.server.fastmcp import FastMCP
import pandas as pd
import sqlite3
from rapidfuzz import process
# Connect to the SQLite database
from pathlib import Path
path = Path(__file__).parent / 'data' / 'gpw_data.sqlite'
conn = sqlite3.connect(str(path))

# Create the MCP server
mcp_app = FastMCP("GPW Data Server", description="Tools for accessing GPW data")


# ---------------------------
# Tool: Get company info by name
# ---------------------------
@mcp_app.tool(
        name="get_company_info",
        description="Fetch company data by company name"
)
def get_company_info(name: str) -> dict:

    company_name = search_companies(name, limit=1)[0][0]

    df = pd.read_sql("SELECT * FROM company_company WHERE name = ?", conn, params=(company_name,))
    return df.iloc[0].to_dict() if not df.empty else {}


# ---------------------------
# Tool: Get shareholders of a company
# ---------------------------
@mcp_app.tool(
        name="get_shareholders",
        description="Return shareholders for a company name."
)
def get_shareholders(name: str) -> dict:
    """Return shareholders for a company as a single dict"""

    company_name = search_companies(name, limit=1)[0][0]  # Get the ticker name from the search result
    df = pd.read_sql("""
        SELECT s.shareholder, s.shares_pct, s.votes_pct
        FROM company_shareholders s
        JOIN company_company c ON s.company_url = c.url
        WHERE c.name = ?
        ORDER BY CAST(s.votes_pct AS FLOAT) DESC
    """, conn, params=(company_name,))

    return df.to_dict(orient="list")



def search_companies(name: str, limit: int = 3, score_cutoff: int = 60) -> list[tuple[str, float]]:
    """Search for company by full name (tolerates typos, returns best match names with scores, outputs ticker name)"""
    df = pd.read_sql("SELECT name, full_name FROM company_company", conn)
    choices = df['full_name'].tolist()
    name = name.lower()
    matches = process.extract(name, [c.lower() for c in choices], limit=limit, score_cutoff=score_cutoff)
    # Map back to original full_name and get corresponding 'name'
    results = []
    for _, score, idx in matches:
        ticker_name = df.iloc[idx]['name']
        results.append((ticker_name, score))
    return results

@mcp_app.tool(
        name="valid_sector_names",
        description="Return all valid sector names available in the database."
)
def valid_sector_names() -> set[str]:
    """Return all sectors available in the database."""
    df = pd.read_sql("SELECT DISTINCT sector FROM company_company", conn)
    return set(df['sector'].tolist())
@mcp_app.tool(
        name="get_sector_companies",
        description="Return all companies with a given valid sector name."
)
def get_sector_companies(sector: str) -> dict:
    """Return all companies in a given sector."""
    df = pd.read_sql("SELECT name, full_name FROM company_company WHERE sector = ?", conn, params=(sector,))
    return df.to_dict(orient="list")

async def run():
    # Run the server as STDIO
    await mcp_app.run_stdio_async()

if __name__ == "__main__":
    import asyncio
    asyncio.run(run())

