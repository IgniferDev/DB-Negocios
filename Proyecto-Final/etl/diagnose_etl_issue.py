# etl/diagnose_etl_issue.py
import os
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine, text

load_dotenv()

SRC_HOST = os.getenv("SRC_HOST")
SRC_USER = os.getenv("SRC_USER")
SRC_PASS = os.getenv("SRC_PASS")
SRC_DB   = os.getenv("SRC_DB")

def make_engine(host,user,password,database):
    if ',' in host:
        s,p = host.split(',',1); hostf = f"{s}:{p}"
    else:
        hostf = host
    return create_engine(f"mssql+pyodbc://{user}:{password}@{hostf}/{database}?driver=ODBC+Driver+17+for+SQL+Server", fast_executemany=True)

def run():
    eng = make_engine(SRC_HOST, SRC_USER, SRC_PASS, SRC_DB)
    print("Connected to source:", SRC_HOST, SRC_DB)
    # projects count
    df_proj = pd.read_sql("SELECT project_id, client_id, team_id, start_date, end_date FROM project", eng)
    print("Projects total:", len(df_proj))
    print(df_proj.head(10).to_string(index=False))

    df_hours = pd.read_sql("""
        SELECT p.project_id,
               ISNULL(SUM(t.hours),0) AS total_hours,
               ISNULL(SUM(t.cost),0) AS total_cost
        FROM project p
        LEFT JOIN time_log t ON p.project_id = t.project_id
        GROUP BY p.project_id
    """, eng)
    print("DF_HOURS rows:", len(df_hours))
    print(df_hours.head(10).to_string(index=False))

    df_rev = pd.read_sql("SELECT project_id, ISNULL(SUM(CASE WHEN type='revenue' THEN amount ELSE 0 END),0) AS total_revenue FROM financials GROUP BY project_id", eng)
    print("DF_REV rows:", len(df_rev))
    print(df_rev.head(10).to_string(index=False))

    df_err = pd.read_sql("SELECT project_id, COUNT(*) AS total_errors FROM [issue] GROUP BY project_id", eng)
    print("DF_ERR rows:", len(df_err))
    print(df_err.head(10).to_string(index=False))

    df_dates = pd.read_sql("SELECT project_id, start_date, end_date, client_id, team_id FROM project", eng)
    print("DF_DATES rows:", len(df_dates))
    print(df_dates.head(10).to_string(index=False))

    # merge to see final shape
    df_fact = df_hours.merge(df_rev, on="project_id", how="left").merge(df_err, on="project_id", how="left").merge(df_dates, on="project_id", how="left")
    print("DF_FACT merged rows:", len(df_fact))
    print(df_fact[['project_id','client_id','team_id','total_hours','total_revenue','total_errors']].head(20).to_string(index=False))

if __name__ == '__main__':
    run()
