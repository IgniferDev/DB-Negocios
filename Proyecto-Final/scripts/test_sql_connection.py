"""
Testeador simple de conexiones a SRC y DST usando pyodbc.
"""

from dotenv import load_dotenv
import os
import pyodbc

load_dotenv()
SRC_HOST = os.getenv("SRC_HOST")
SRC_USER = os.getenv("SRC_USER")
SRC_PASS = os.getenv("SRC_PASS")
SRC_DB = os.getenv("SRC_DB")
DST_HOST = os.getenv("DST_HOST")
DST_USER = os.getenv("DST_USER")
DST_PASS = os.getenv("DST_PASS")
DST_DB = os.getenv("DST_DB")
DRIVER = "ODBC Driver 17 for SQL Server"

def test_conn(host,user,pwd,db):
    conn_str = f"DRIVER={{{DRIVER}}};SERVER={host};DATABASE={db};UID={user};PWD={pwd}"
    try:
        cn = pyodbc.connect(conn_str, timeout=5)
        print(f"Connected to {db} at {host} as {user}")
        cn.close()
    except Exception as e:
        print(f"Failed to connect to {db} at {host} as {user}: {e}")

if __name__ == "__main__":
    print("Testing SRC...")
    test_conn(SRC_HOST,SRC_USER,SRC_PASS,SRC_DB)
    print("Testing DST...")
    test_conn(DST_HOST,DST_USER,DST_PASS,DST_DB)
