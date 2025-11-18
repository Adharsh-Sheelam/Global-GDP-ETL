import requests
import pandas as pd
import sqlite3
import logging
from bs4 import BeautifulSoup

# ---------------------- LOGGING SETUP ----------------------
logging.basicConfig(
    filename="etl_project_log.txt",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logging.info("ETL Job Started")

# ---------------------- URL SOURCE ----------------------
url = "https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29"
try:
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    logging.info("URL fetched successfully.")
    print("✓ URL fetched successfully")
except Exception as e:
    logging.error(f"Error fetching URL: {e}")
    print(f"ERROR: Could not fetch URL - {e}")
    raise SystemExit(e)

# ---------------------- EXTRACT ----------------------
soup = BeautifulSoup(response.text, "html.parser")
tables = soup.find_all("table", {"class": "wikitable"})

if not tables:
    logging.error("No wikitable found on the page")
    print("ERROR: No wikitable found")
    raise SystemExit("No wikitable found")

# Try to find the right table (usually the first one with country GDP data)
df = None
for i, table in enumerate(tables):
    try:
        from io import StringIO
        temp_df = pd.read_html(StringIO(str(table)))[0]
        if len(temp_df) > 50:  # GDP tables typically have many rows
            df = temp_df
            print(f"✓ Found data table (Table #{i}) with {len(temp_df)} rows")
            break
    except:
        continue

if df is None:
    from io import StringIO
    df = pd.read_html(StringIO(str(tables[0])))[0]

logging.info("HTML table extracted successfully.")

# ---------------------- TRANSFORM ----------------------
# Handle multi-level headers (tuple column names)
df.columns = [
    " ".join(map(str, col)) if isinstance(col, tuple) else str(col) 
    for col in df.columns
]

# Clean weird unicode spaces and extra spaces
df.columns = [col.replace("\xa0", " ").strip().replace("  ", " ") for col in df.columns]

# Print columns for debugging
print("\n" + "="*60)
print("DETECTED COLUMNS:")
print("="*60)
for i, col in enumerate(df.columns):
    print(f"{i}: '{col}'")
print("="*60 + "\n")

# ---------------------- SMART COLUMN DETECTION ----------------------
# Find country column
country_col = None
country_keywords = ['country', 'territory', 'nation', 'region']
for col in df.columns:
    col_lower = col.lower()
    if any(keyword in col_lower for keyword in country_keywords):
        country_col = col
        print(f"✓ Found Country Column: '{col}'")
        break

# Find GDP column (look for IMF, World Bank, or UN estimates)
gdp_col = None
gdp_keywords = ['estimate', 'million', 'gdp', 'usd', 'us$', 'imf']
for col in df.columns:
    col_lower = col.lower()
    if any(keyword in col_lower for keyword in gdp_keywords):
        # Skip columns with 'year' in them
        if 'year' not in col_lower:
            gdp_col = col
            print(f"✓ Found GDP Column: '{col}'")
            break

# Validate columns were found
if not country_col:
    print("\nERROR: Could not find Country column!")
    print("Available columns:", list(df.columns))
    logging.error("Country column not found")
    raise SystemExit("Country column detection failed")

if not gdp_col:
    print("\nERROR: Could not find GDP column!")
    print("Available columns:", list(df.columns))
    logging.error("GDP column not found")
    raise SystemExit("GDP column detection failed")

print()

# Reduce dataframe to required columns
df = df[[country_col, gdp_col]].copy()

# Clean GDP column - handle non-numeric values
print("Cleaning GDP data...")
df[gdp_col] = (
    df[gdp_col]
    .astype(str)
    .str.replace(",", "")
    .str.replace("—", "")
    .str.replace("−", "")
    .str.replace(r"[^\d.]", "", regex=True)
)

# Convert to float, handling any conversion errors
df[gdp_col] = pd.to_numeric(df[gdp_col], errors='coerce')

# Remove rows with NaN or zero values
df = df.dropna()
df = df[df[gdp_col] > 0]

# Convert to billions
df["GDP_USD_billion"] = (df[gdp_col] / 1000).round(2)

# Rename final columns
df = df.rename(columns={country_col: "Country"})
df = df[["Country", "GDP_USD_billion"]]

logging.info("Data transformed successfully.")
print(f"✓ Data transformed - {len(df)} countries processed\n")

# ---------------------- LOAD → CSV ----------------------
csv_filename = "Countries_by_GDP.csv"
df.to_csv(csv_filename, index=False)
logging.info(f"CSV saved as {csv_filename}")
print(f"✓ CSV saved: {csv_filename}")

# ---------------------- LOAD → SQLITE DATABASE ----------------------
db_name = "World_Economies.db"
conn = sqlite3.connect(db_name)
cursor = conn.cursor()

cursor.execute("DROP TABLE IF EXISTS Countries_by_GDP")
cursor.execute("""
    CREATE TABLE Countries_by_GDP(
        Country TEXT,
        GDP_USD_billion REAL
    )
""")

df.to_sql("Countries_by_GDP", conn, if_exists="append", index=False)
logging.info("Data loaded into SQLite database.")
print(f"✓ Database created: {db_name}\n")

# ---------------------- QUERY (>100 billion GDP) ----------------------
query = """
SELECT * FROM Countries_by_GDP
WHERE GDP_USD_billion > 100
ORDER BY GDP_USD_billion DESC;
"""
result_df = pd.read_sql(query, conn)
logging.info("Query executed successfully.")

conn.close()

# ---------------------- PRINT RESULT ----------------------
print("="*60)
print("COUNTRIES WITH GDP > 100 BILLION USD")
print("="*60)
print(result_df.to_string(index=False))
print("="*60)
print(f"Total countries: {len(result_df)}")
print("="*60)

logging.info("ETL Job Completed Successfully")
print("\n✓ ETL Job Completed Successfully!")