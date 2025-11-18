**Global GDP ETL Pipeline**
A complete ETL (Extract–Transform–Load) pipeline built in Python that scrapes global GDP (Nominal) data from Wikipedia, cleans and transforms it using Pandas, and loads the final dataset into CSV and SQLite for analysis. This project demonstrates end-to-end data engineering skills including web scraping, data cleaning, data modeling, SQL querying, and automation.

**Project Overview**
This pipeline extracts raw GDP data from Wikipedia (via the Wayback Machine), processes it into clean structured data, and stores the final output in both CSV and SQLite database formats.
It also generates insights such as countries with GDP > 100 billion USD.

**Technologies Used:**
Python 3
Pandas
BeautifulSoup (bs4)
Requests
SQLite3
Logging module
LXML (for HTML parsing)

**Features**
🌐 Web scraping GDP data from archived Wikipedia pages
🔍 Automatic detection of the correct GDP table
🛠️ Advanced data cleaning & transformation
💾 Load to CSV and SQLite database
📊 Run SQL queries for insights
🧩 Full logging for debugging and transparency
🧹 Handles messy HTML tables, special characters & inconsistent headers

**📁 Output Files**
Countries_by_GDP.csv – Clean final dataset
World_Economies.db – SQLite database with GDP table
etl_project_log.txt – Execution logs
SQL result: Countries with GDP > 100B USD
