#!/usr/bin/env python
"""
generate_data_sqlserver.py
Generador de datos sintéticos e inserción en SQL Server (PROJECT_MANAGE).
Uso: python generate_data_sqlserver.py [--clean]

- Si se pasa --clean: limpia la BD origen (Ponki) de forma segura antes de insertar.
- Si no se pasa --clean: realiza inserciones idempotentes (no duplicará PK existentes).
"""

import argparse
import os
import random
from datetime import date, timedelta
from faker import Faker
import numpy as np
import pyodbc
from dotenv import load_dotenv

load_dotenv()
fake = Faker()
random.seed(42)
np.random.seed(42)
Faker.seed(42)

# Config desde environment (si no existen, defaults)
SRC_HOST = os.getenv("SRC_HOST", "172.24.56.35,1433")
SRC_USER = os.getenv("SRC_USER", "Inteligencia")
SRC_PASS = os.getenv("SRC_PASS", "Rock2213#")
SRC_DB   = os.getenv("SRC_DB", "PROJECT_MANAGE")

N_CLIENTS = int(os.getenv("N_CLIENTS", 8))
N_TEAMS = int(os.getenv("N_TEAMS", 4))
N_EMPLOYEES = int(os.getenv("N_EMPLOYEES", 24))
N_PROJECTS = int(os.getenv("N_PROJECTS", 20))
DATE_MIN = date(2020,1,1)
DATE_MAX = date(2025,12,31)

DRIVER = "ODBC Driver 17 for SQL Server"

def get_conn():
    conn_str = f"DRIVER={{{DRIVER}}};SERVER={SRC_HOST};DATABASE={SRC_DB};UID={SRC_USER};PWD={SRC_PASS}"
    return pyodbc.connect(conn_str, autocommit=True)

def clean_db(cn):
    """
    Limpieza segura y ordenada que evita violaciones de FK:
    - pone a NULL references a project_stage desde project
    - borra tablas en orden hijo->padre
    - reseedea identity donde aplique
    """
    cur = cn.cursor()
    print("Starting safe clean of source DB...")

    # 1) Evitar FK: setear a NULL current_stage_id en project (si existe)
    try:
        cur.execute("IF COL_LENGTH('project','current_stage_id') IS NOT NULL BEGIN UPDATE project SET current_stage_id = NULL WHERE current_stage_id IS NOT NULL END")
        print("project.current_stage_id set to NULL (if existed).")
    except Exception as e:
        print("Warning: no se pudo setear current_stage_id a NULL:", e)

    # 2) Delete en orden seguro: hijos primero
    tables_order = ["time_log","[issue]","financials","team_member","project_stage","project","employee","[team]","client","[stage]"]
    for t in tables_order:
        try:
            cur.execute(f"IF OBJECT_ID('{t}') IS NOT NULL DELETE FROM {t}")
            print(f"Deleted table {t} (if existed).")
        except Exception as e:
            print(f"Warning deleting {t}:", e)

    # 3) Reseed identities (si existen)
    reseeds = ['project_stage','team_member','time_log','[issue]','financials']
    for r in reseeds:
        try:
            cur.execute(f"DBCC CHECKIDENT ('{r}', RESEED, 0)")
            print(f"Reseeded {r}")
        except Exception as e:
            print(f"Warning reseeding {r}:", e)

    cur.commit()
    print("DB cleaned (safe).")

def insert_if_not_exists(cur, table, key_col, row_tuple, columns):
    """
    Inserta fila si no existe. 
    - cur: pyodbc cursor
    - table: tabla destino
    - key_col: nombre de la columna PK (ej 'client_id')
    - row_tuple: tupla con valores en el mismo orden que columns
    - columns: lista de nombres de columnas
    """
    key_val = row_tuple[0]
    placeholders = ", ".join(["?"] * len(columns))
    cols_sql = ", ".join(columns)
    sql = f"IF NOT EXISTS (SELECT 1 FROM {table} WHERE {key_col} = ?) INSERT INTO {table} ({cols_sql}) VALUES ({placeholders})"
    params = (key_val,) + row_tuple
    cur.execute(sql, params)

def generate(clean=False):
    cn = get_conn()
    cur = cn.cursor()
    print("Conectado a source", SRC_HOST)

    if clean:
        clean_db(cn)

    # 1. clients
    clients = []
    for cid in range(1, N_CLIENTS+1):
        clients.append((cid, fake.company(), random.choice(["Finance","Retail","Health","Education","Energy","Telecom"]), random.choices(["Gold","Silver","Bronze"], weights=[0.2,0.5,0.3])[0]))

    cur.fast_executemany = False
    for c in clients:
        insert_if_not_exists(cur, "client", "client_id", c, ["client_id","client_name","industry","tier"])
    print(f"Processed {len(clients)} clients")

    # 2. teams
    teams = [(i, f"Team-{i}", fake.bs()) for i in range(1, N_TEAMS+1)]
    for t in teams:
        insert_if_not_exists(cur, "team", "team_id", t, ["team_id","team_name","description"])
    print(f"Processed {len(teams)} teams")

    # 3. stages (static 5)
    stages = [(1,"Planning","Planning stage"), (2,"Design","Design stage"), (3,"Development","Development stage"), (4,"Testing","Testing stage"), (5,"Deployment","Deployment stage")]
    for s in stages:
        insert_if_not_exists(cur, "stage", "stage_id", s, ["stage_id","stage_name","description"])
    print("Processed stages")

    # 4. employees
    roles = ["Developer","QA","PM","DevOps","Analyst"]
    base_cost = {"Developer":30,"QA":25,"PM":45,"DevOps":35,"Analyst":28}
    employees = []
    for eid in range(1, N_EMPLOYEES+1):
        name = fake.name()
        email = name.lower().replace(" ", ".") + f"{eid}@example.com"
        role = random.choices(roles, weights=[0.45,0.20,0.10,0.10,0.15])[0]
        hire_date = fake.date_between_dates(date_start=date(2018,1,1), date_end=date.today())
        hourly = round(max(10, np.random.normal(base_cost[role],5)),2)
        capacity_weekly = 40.0
        employees.append((eid, name, email, role, hire_date, hourly, capacity_weekly))
    for e in employees:
        insert_if_not_exists(cur, "employee", "employee_id", e, ["employee_id","name","email","role","hire_date","hourly_cost","capacity_weekly"])
    print(f"Processed {len(employees)} employees")

    # 5. projects + project_stage + team_member + time_log + issue + financials
    project_rows = []
    project_stage_rows = []
    team_member_rows = []
    time_log_rows = []
    issue_rows = []
    financial_rows = []

    emp_ids = [e[0] for e in employees]
    team_ids = [t[0] for t in teams]
    client_ids = [c[0] for c in clients]

    for pid in range(1, N_PROJECTS+1):
        pname = f"Project {pid} - {fake.bs()[:30]}"
        delta = (DATE_MAX - DATE_MIN).days
        sdate = DATE_MIN + timedelta(days=random.randint(0, max(1, delta-60)))
        duration = max(30, int(abs(np.random.normal(90,40))))
        edate = sdate + timedelta(days=duration)
        client = random.choice(client_ids)
        team = random.choice(team_ids)
        budget = round(abs(np.random.normal(60000,30000)) + 5000,2)
        priority = random.choices(["High","Medium","Low"], weights=[0.2,0.5,0.3])[0]
        project_rows.append((pid, pname, fake.text(max_nb_chars=200), sdate, edate, client, team, budget, priority, None))

        st_start = sdate
        for st in [1,2,3,4,5]:
            st_len = max(5, int(duration/5 + random.randint(-3,3)))
            st_end = st_start + timedelta(days=st_len)
            project_stage_rows.append((pid, st, st_start, st_end))
            lam = 0.05
            if st == 3: lam = 0.7
            if st == 4: lam = 1.0
            weeks = max(1, (st_end - st_start).days // 7)
            for w in range(weeks):
                n_iss = np.random.poisson(lam)
                for _ in range(n_iss):
                    d = st_start + timedelta(days=random.randint(0, max(0, (st_end-st_start).days)))
                    sev = random.choices(["Low","Medium","High"], weights=[0.6,0.3,0.1])[0]
                    issue_rows.append((pid, None, sev, fake.catch_phrase(), fake.text(max_nb_chars=200), d))
            st_start = st_end + timedelta(days=1)

        tm = random.sample(emp_ids, k=random.randint(3,6))
        for e in tm:
            assigned_date = sdate - timedelta(days=random.randint(0,30))
            role_in_team = random.choice(["Dev","QA","Lead","Support"])
            team_member_rows.append((team, e, assigned_date, role_in_team))

        for e in tm:
            dcur = sdate
            while dcur <= edate:
                if dcur.weekday() < 5:
                    hours = float(round(np.clip(np.random.normal(6.5,1.2), 0.5, 12.0),2))
                    time_log_rows.append((pid, e, hours, 0.0, dcur))
                dcur += timedelta(days=1)

        rev = round(budget * (1 + np.random.normal(0.05,0.08)),2)
        financial_rows.append((pid, rev, "revenue", "USD", sdate))
        for _ in range(random.randint(1,4)):
            financial_rows.append((pid, round(abs(np.random.normal(2000,1500)),2), "expense", "USD", sdate + timedelta(days=random.randint(0,duration))))

    # Insert projects (idempotent)
    for p in project_rows:
        pid_val = p[0]
        cur.execute(
            "IF NOT EXISTS (SELECT 1 FROM project WHERE project_id = ?) "
            "INSERT INTO project (project_id, project_name, description, start_date, end_date, client_id, team_id, budget, priority, current_stage_id) "
            "VALUES (?,?,?,?,?,?,?,?,?,?)",
            (pid_val, p[1], p[2], p[3], p[4], p[5], p[6], p[7], p[8], p[9])
        )
    print(f"Processed {len(project_rows)} projects (idempotent)")

    # Insert project_stage rows
    for ps in project_stage_rows:
        cur.execute("INSERT INTO project_stage (project_id, stage_id, start_date, end_date) VALUES (?, ?, ?, ?)", ps)
    print(f"Inserted {len(project_stage_rows)} project_stage rows (duplicates may occur if re-run; consider --clean)")

    # Update current_stage_id to last stage id for each project
    try:
        rows = cur.execute("SELECT id, project_id FROM project_stage ORDER BY id").fetchall()
        mapping = {}
        for r in rows:
            pid_r = r[1]
            mapping.setdefault(pid_r, []).append(r[0])
        for pid_k, lst in mapping.items():
            last_id = lst[-1]
            cur.execute("UPDATE project SET current_stage_id = ? WHERE project_id = ?", (last_id, pid_k))
    except Exception as e:
        print("Warning updating current_stage_id:", e)

    # team_member - insert idempotently
    unique_tm = list({(tm[0],tm[1],tm[2],tm[3]) for tm in team_member_rows})
    for tm in unique_tm:
        cur.execute(
            "IF NOT EXISTS (SELECT 1 FROM team_member WHERE team_id=? AND employee_id=? AND assigned_date=? AND role_in_team=?) "
            "INSERT INTO team_member (team_id, employee_id, assigned_date, role_in_team) VALUES (?, ?, ?, ?)",
            (tm[0], tm[1], tm[2], tm[3], tm[0], tm[1], tm[2], tm[3])
        )
    print(f"Processed {len(unique_tm)} team_member rows (idempotent)")

    # Issues mapping to project_stage ids
    ps_rows = cur.execute("SELECT id, project_id FROM project_stage").fetchall()
    ps_map = {}
    for r in ps_rows:
        ps_map.setdefault(r[1], []).append(r[0])
    final_issues = []
    for iss in issue_rows:
        pid_i = iss[0]
        possible = ps_map.get(pid_i, [])
        chosen = random.choice(possible) if possible else None
        final_issues.append((pid_i, chosen, iss[2], iss[3], iss[4], iss[5]))
    for fi in final_issues:
        cur.execute("INSERT INTO [issue] (project_id, project_stage_id, severity, concept, error_message, date_reported) VALUES (?, ?, ?, ?, ?, ?)", fi)
    print(f"Inserted {len(final_issues)} issues (may produce duplicates if re-run without --clean)")

    # time_log: compute cost by employee hourly
    emp_costs = {}
    try:
        rows = cur.execute("SELECT employee_id, hourly_cost FROM employee").fetchall()
        emp_costs = {r[0]: float(r[1]) for r in rows}
    except Exception as e:
        print("Warning fetching employee costs:", e)
    final_tl = []
    for tl in time_log_rows:
        pid_t, eid_t, hrs, _, d = tl
        cost = round(hrs * emp_costs.get(eid_t, 30.0), 2)
        final_tl.append((pid_t, eid_t, hrs, cost, d))
    for tl in final_tl:
        cur.execute(
            "IF NOT EXISTS (SELECT 1 FROM time_log WHERE project_id=? AND employee_id=? AND log_date=? AND hours=?) "
            "INSERT INTO time_log (project_id, employee_id, hours, cost, log_date) VALUES (?,?,?,?,?)",
            (tl[0], tl[1], tl[4], tl[2], tl[0], tl[1], tl[2], tl[3], tl[4])
        )
    print(f"Processed {len(final_tl)} time_log rows (idempotent)")

    # financials: idempotent by project_id, amount, type, date_recorded
    for fin in financial_rows:
        cur.execute(
            "IF NOT EXISTS (SELECT 1 FROM financials WHERE project_id=? AND amount=? AND type=? AND date_recorded=?) "
            "INSERT INTO financials (project_id, amount, type, currency, date_recorded) VALUES (?, ?, ?, ?, ?)",
            (fin[0], fin[1], fin[2], fin[4], fin[0], fin[1], fin[2], fin[3], fin[4])
        )
    print(f"Processed {len(financial_rows)} financial rows (idempotent)")

    cur.commit()
    print("Generación finalizada.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--clean', action='store_true', help='Clean source DB before inserting')
    args = parser.parse_args()
    generate(clean=args.clean)
