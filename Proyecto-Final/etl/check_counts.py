# etl/check_counts.py
import os
import pyodbc
from dotenv import load_dotenv
load_dotenv()

DRIVER = "ODBC Driver 17 for SQL Server"
SRC_HOST = os.getenv("SRC_HOST", "172.24.56.35,1433")
SRC_DB   = os.getenv("SRC_DB", "PROJECT_MANAGE")
SRC_USER = os.getenv("SRC_USER")
SRC_PASS = os.getenv("SRC_PASS")

conn_str = f"DRIVER={{{DRIVER}}};SERVER={SRC_HOST};DATABASE={SRC_DB};UID={SRC_USER};PWD={SRC_PASS}"
cn = pyodbc.connect(conn_str, autocommit=True, timeout=5)
cur = cn.cursor()

tables = ["client","team","employee","stage","project","project_stage","team_member","time_log","issue","financials"]
for t in tables:
    try:
        cur.execute(f"SELECT COUNT(*) FROM dbo.{t}")
        cnt = cur.fetchone()[0]
    except Exception as e:
        cnt = f"ERROR: {e}"
    print(f"{t:14} -> {cnt}")
cn.close()
