# etl_load_dw_sqlserver_fixed.py
import os, time
from datetime import datetime, timedelta
import pyodbc
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()

SRC = {"host": os.getenv("SRC_HOST", "172.24.56.35,1433"), "user": os.getenv("SRC_USER","Inteligencia"), "pass": os.getenv("SRC_PASS","Rock2213#"), "db": os.getenv("SRC_DB","PROJECT_MANAGE")}
DST = {"host": os.getenv("DST_HOST", "172.24.84.67,1433"), "user": os.getenv("DST_USER","admin_kiry2"), "pass": os.getenv("DST_PASS","1234"), "db": os.getenv("DST_DB","PROJECT_SUPPORT_SYSTEM")}
DRIVER = "ODBC Driver 17 for SQL Server"

def create_sqlalchemy_engine(host, user, password, database):
    # host expected like "10.147.17.5,1433" -> SQLAlchemy dialect requires replacing comma with colon for port
    if ',' in host:
        server, port = host.split(',',1)
        host_formatted = f"{server}:{port}"
    else:
        host_formatted = host
    # Using pyodbc + DSN-less connection string
    conn_str = f"mssql+pyodbc://{user}:{password}@{host_formatted}/{database}?driver=ODBC+Driver+17+for+SQL+Server"
    # If there are URL encoding issues, you can use urllib.parse.quote_plus
    return create_engine(conn_str, fast_executemany=True)

def conn_pyodbc(cfg):
    conn_str = f"DRIVER={{{DRIVER}}};SERVER={cfg['host']};DATABASE={cfg['db']};UID={cfg['user']};PWD={cfg['pass']}"
    return pyodbc.connect(conn_str, autocommit=True)

def ensure_etl_audit(dst_cn):
    dst_cur = dst_cn.cursor()
    dst_cur.execute("""
    IF OBJECT_ID('dbo.etl_audit') IS NULL
    CREATE TABLE dbo.etl_audit (
      run_id DATETIME PRIMARY KEY,
      src_rows_time_log INT,
      src_rows_financials INT,
      src_rows_issue INT,
      dst_rows_dim_client INT,
      dst_rows_dim_team INT,
      dst_rows_dim_project INT,
      dst_rows_fact_project INT,
      duration_seconds FLOAT,
      status VARCHAR(20),
      notes NVARCHAR(1000)
    );
    """)

def upsert_dim_date(dst_cn, start_date, end_date):
    cur = dst_cn.cursor()
    dates = []
    d = start_date
    while d <= end_date:
        date_id = int(d.strftime("%Y%m%d"))
        dates.append((date_id, d, d.year, (d.month-1)//3+1, d.month, d.day, d.strftime("%A")))
        d += timedelta(days=1)
    cur.execute("IF OBJECT_ID('tempdb..#tmp_dates') IS NOT NULL DROP TABLE #tmp_dates")
    cur.execute("CREATE TABLE #tmp_dates (date_id INT, full_date DATE, [year] INT, quarter INT, [month] INT, [day] INT, weekday VARCHAR(20))")
    cur.fast_executemany = True
    cur.executemany("INSERT INTO #tmp_dates VALUES (?, ?, ?, ?, ?, ?, ?)", dates)
    cur.execute("""
    MERGE INTO dim_date AS T
    USING #tmp_dates AS S
    ON T.date_id = S.date_id
    WHEN NOT MATCHED BY TARGET THEN
      INSERT (date_id, full_date, year, quarter, month, day, weekday)
      VALUES (S.date_id, S.full_date, S.year, S.quarter, S.month, S.day, S.weekday);""")
    cur.execute("DROP TABLE #tmp_dates")

def upsert_simple_table(dst_cn, df, table_name, key):
    cur = dst_cn.cursor()
    cols = df.columns.tolist()
    # temp table create
    tmp_name = f"#tmp_{table_name}"
    cur.execute(f"IF OBJECT_ID('tempdb..{tmp_name}') IS NOT NULL DROP TABLE {tmp_name}")
    create = "CREATE TABLE " + tmp_name + " (" + ",".join([f"{c} NVARCHAR(4000)" for c in cols]) + ")"
    cur.execute(create)
    rows = [tuple(str(x) if x is not None else None for x in row) for row in df.values.tolist()]
    placeholders = "(" + ",".join(["?"]*len(cols)) + ")"
    if rows:
        cur.fast_executemany = True
        cur.executemany(f"INSERT INTO {tmp_name} VALUES {placeholders}", rows)
    # merge - insert only for simplicity
    merge_sql = f"""
    MERGE INTO {table_name} AS T
    USING {tmp_name} AS S
    ON T.{key} = S.{key}
    WHEN NOT MATCHED BY TARGET THEN
      INSERT ({','.join(cols)}) VALUES ({','.join(['S.'+c for c in cols])});
    """
    cur.execute(merge_sql)
    cur.execute(f"DROP TABLE {tmp_name}")

def main():
    start_time = time.time()
    run_ts = datetime.now()
    status = "OK"
    notes = ""
    try:
        # create engines for pandas
        src_engine = create_sqlalchemy_engine(SRC['host'], SRC['user'], SRC['pass'], SRC['db'])
        dst_engine = create_sqlalchemy_engine(DST['host'], DST['user'], DST['pass'], DST['db'])
        # also keep pyodbc connection for fast executemany & merges
        src_cn = conn_pyodbc(SRC)
        dst_cn = conn_pyodbc(DST)
        print("Connected to source and destination (engines ready).")
        ensure_etl_audit(dst_cn)

        # fecha rango
        q = "SELECT MIN(start_date) AS min_s, MAX(end_date) AS max_e FROM project"
        r = pd.read_sql(q, src_engine).iloc[0]
        min_date = r['min_s'] if pd.notnull(r['min_s']) else datetime(2023,1,1)
        max_date = r['max_e'] if pd.notnull(r['max_e']) else datetime.today()
        if isinstance(min_date, str): min_date = pd.to_datetime(min_date)
        if isinstance(max_date, str): max_date = pd.to_datetime(max_date)
        min_date = pd.to_datetime(min_date) - timedelta(days=30)
        max_date = pd.to_datetime(max_date) + timedelta(days=30)
        upsert_dim_date(dst_cn, min_date.date(), max_date.date())

        # dims via pandas (using engines avoids warnings)
        df_client = pd.read_sql("SELECT client_id, client_name, industry, tier FROM client", src_engine)
        upsert_simple_table(dst_cn, df_client, "dim_client", "client_id")

        df_team = pd.read_sql("SELECT team_id, team_name, description FROM team", src_engine)
        upsert_simple_table(dst_cn, df_team, "dim_team", "team_id")

        df_proj = pd.read_sql("SELECT project_id, project_name, priority, budget FROM project", src_engine)
        upsert_simple_table(dst_cn, df_proj, "dim_project", "project_id")

        # metrics count
        src_tl = int(pd.read_sql("SELECT COUNT(*) AS cnt FROM time_log", src_engine).iloc[0]['cnt'])
        src_fin = int(pd.read_sql("SELECT COUNT(*) AS cnt FROM financials", src_engine).iloc[0]['cnt'])
        src_issue = int(pd.read_sql("SELECT COUNT(*) AS cnt FROM issue", src_engine).iloc[0]['cnt'])

        # Aggregations
        df_hours = pd.read_sql("""
            SELECT p.project_id,
                ISNULL(SUM(t.hours),0) AS total_hours,
                ISNULL(SUM(t.cost),0) AS total_cost
            FROM project p
            LEFT JOIN time_log t ON p.project_id = t.project_id
            GROUP BY p.project_id
        """, src_engine)

        df_rev = pd.read_sql("SELECT project_id, ISNULL(SUM(CASE WHEN type='revenue' THEN amount ELSE 0 END),0) AS total_revenue FROM financials GROUP BY project_id", src_engine)
        df_err = pd.read_sql("SELECT project_id, COUNT(*) AS total_errors FROM issue GROUP BY project_id", src_engine)

        df_dates = pd.read_sql("SELECT project_id, start_date, end_date, client_id, team_id FROM project", src_engine)
        # Convertir a datetime robusto
        df_dates['start_date'] = pd.to_datetime(df_dates['start_date'], errors='coerce').fillna(pd.to_datetime('2023-01-01'))
        df_dates['end_date']   = pd.to_datetime(df_dates['end_date'], errors='coerce').fillna(pd.to_datetime('2023-01-01'))
        df_dates['start_date_id'] = df_dates['start_date'].dt.strftime("%Y%m%d").astype(int)
        df_dates['end_date_id']   = df_dates['end_date'].dt.strftime("%Y%m%d").astype(int)

        # Ensure non-empty frames for safe merges
        for df in (df_hours, df_rev, df_err):
            if df is None or df.empty:
                # create empty with expected columns
                if df is df_hours:
                    df_hours = pd.DataFrame(columns=['project_id','total_hours','total_cost'])
                elif df is df_rev:
                    df_rev = pd.DataFrame(columns=['project_id','total_revenue'])
                elif df is df_err:
                    df_err = pd.DataFrame(columns=['project_id','total_errors'])

        # merge
        df_fact = df_hours.merge(df_rev, on="project_id", how="left").merge(df_err, on="project_id", how="left").merge(df_dates, on="project_id", how="left")
        df_fact["total_revenue"] = df_fact["total_revenue"].fillna(0)
        df_fact["total_errors"] = df_fact["total_errors"].fillna(0).astype(int)
        df_fact["total_hours"] = df_fact["total_hours"].fillna(0)
        df_fact["total_cost"] = df_fact["total_cost"].fillna(0)

        # prepare rows
        rows = []
        for _, r in df_fact.iterrows():
            rows.append((int(r.project_id), int(r.client_id), int(r.team_id), int(r.start_date_id), int(r.end_date_id),
                         float(r.total_hours), float(r.total_cost), float(r.total_revenue), int(r.total_errors)))

        dst_cn_cur = dst_cn.cursor()
        if rows:
            proj_ids = ",".join(str(x[0]) for x in rows)
            dst_cn_cur.execute(f"DELETE FROM fact_project WHERE project_id IN ({proj_ids})")
            insert_sql = """INSERT INTO fact_project (project_id, client_id, team_id, start_date_id, end_date_id, total_hours, total_cost, total_revenue, total_errors)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"""
            dst_cn_cur.fast_executemany = True
            dst_cn_cur.executemany(insert_sql, rows)
            dst_cn.commit()

        duration = time.time() - start_time
        # audit
        dst_cn.cursor().execute("INSERT INTO etl_audit (run_id, src_rows_time_log, src_rows_financials, src_rows_issue, dst_rows_dim_client, dst_rows_dim_team, dst_rows_dim_project, dst_rows_fact_project, duration_seconds, status, notes) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                                (run_ts, src_tl, src_fin, src_issue, len(df_client), len(df_team), len(df_proj), len(rows), duration, "OK", "ETL ejecutado correctamente"))
        print("ETL finalizado. Projects processed:", len(rows))
    except Exception as e:
        status = "ERROR"
        notes = str(e)
        print("ERROR ETL:", e)
        # Try to log error if dst available
        try:
            dst_cn.cursor().execute("INSERT INTO etl_audit (run_id, duration_seconds, status, notes) VALUES (?,?,?,?)", (run_ts, time.time()-start_time, status, notes))
        except Exception:
            pass

if __name__ == "__main__":
    main()
