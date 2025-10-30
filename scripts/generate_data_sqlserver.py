# generate_data_sqlserver.py
# Generador de datos sintéticos e inserción en SQL Server (PROJECT_MANAGE en Ponki).
# Ejecutar desde Ignifer. Usa .env para las credenciales.

import os, random, math
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

N_CLIENTS = int(os.getenv("N_CLIENTS", 16))
N_TEAMS = int(os.getenv("N_TEAMS", 8))
N_EMPLOYEES = int(os.getenv("N_EMPLOYEES", 48))
N_PROJECTS = int(os.getenv("N_PROJECTS", 60))
DATE_MIN = date(2020,1,1)
DATE_MAX = date(2025,12,31)

DRIVER = "ODBC Driver 17 for SQL Server"

def get_conn():
    conn_str = f"DRIVER={{{DRIVER}}};SERVER={SRC_HOST};DATABASE={SRC_DB};UID={SRC_USER};PWD={SRC_PASS}"
    return pyodbc.connect(conn_str, autocommit=True)

def truncate_if_exists(cur, table):
    try:
        cur.execute(f"DELETE FROM {table};")
    except Exception as e:
        print("Warning truncating", table, e)

def generate():
    cn = get_conn()
    cur = cn.cursor()
    print("Conectado a source", SRC_HOST)

    # Opcional: limpiar tablas (descomenta si quieres empezar limpio)
    # tables = ["time_log","team_member","project_stage","project","financials","issue","employee","team","client","stage"]
    # for t in tables:
    #     truncate_if_exists(cur, t)

    # 1. clients
    clients = []
    for cid in range(1, N_CLIENTS+1):
        clients.append((cid, fake.company(), random.choice(["Finance","Retail","Health","Education","Energy","Telecom"]), random.choices(["Gold","Silver","Bronze"], weights=[0.2,0.5,0.3])[0]))
    cur.fast_executemany = True
    cur.executemany("INSERT INTO client (client_id, client_name, industry, tier) VALUES (?, ?, ?, ?)", clients)
    print(f"Inserted {len(clients)} clients")

    # 2. teams
    teams = [(i, f"Team-{i}", fake.bs()) for i in range(1, N_TEAMS+1)]
    cur.executemany("INSERT INTO team (team_id, team_name, description) VALUES (?, ?, ?)", teams)
    print(f"Inserted {len(teams)} teams")

    # 3. stages (static 5)
    stages = [(1,"Planning","Planning stage"), (2,"Design","Design stage"), (3,"Development","Development stage"), (4,"Testing","Testing stage"), (5,"Deployment","Deployment stage")]
    cur.executemany("INSERT INTO stage (stage_id, stage_name, description) VALUES (?, ?, ?)", stages)
    print("Inserted stages")

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
    cur.executemany("INSERT INTO employee (employee_id, name, email, role, hire_date, hourly_cost, capacity_weekly) VALUES (?, ?, ?, ?, ?, ?, ?)", employees)
    print(f"Inserted {len(employees)} employees")

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

    pid = 1
    for pid in range(1, N_PROJECTS+1):
        pname = f"Project {pid} - {fake.bs()[:30]}"
        delta = (DATE_MAX - DATE_MIN).days
        sdate = DATE_MIN + timedelta(days=random.randint(0, max(1, delta-60)))
        duration = max(30, int(abs(np.random.normal(90,40))) )  # days
        edate = sdate + timedelta(days=duration)
        client = random.choice(client_ids)
        team = random.choice(team_ids)
        budget = round(abs(np.random.normal(60000,30000)) + 5000,2)
        priority = random.choices(["High","Medium","Low"], weights=[0.2,0.5,0.3])[0]
        project_rows.append((pid, pname, fake.text(max_nb_chars=200), sdate, edate, client, team, budget, priority, None))

        # stages sequentiales
        st_start = sdate
        for st in [1,2,3,4,5]:
            st_len = max(5, int(duration/5 + random.randint(-3,3)))
            st_end = st_start + timedelta(days=st_len)
            project_stage_rows.append((pid, st, st_start, st_end))
            # issues (Poisson)
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

        # assign some team members
        tm = random.sample(emp_ids, k=random.randint(3,6))
        for e in tm:
            assigned_date = sdate - timedelta(days=random.randint(0,30))
            role_in_team = random.choice(["Dev","QA","Lead","Support"])
            team_member_rows.append((team, e, assigned_date, role_in_team))

        # time logs per working day for each team member
        for e in tm:
            dcur = sdate
            while dcur <= edate:
                if dcur.weekday() < 5:
                    hours = float(round(np.clip(np.random.normal(6.5,1.2), 0.5, 12.0),2))
                    # fetch hourly later: placeholder 0. We'll map to employee hourly later
                    time_log_rows.append((pid, e, hours, 0.0, dcur))
                dcur += timedelta(days=1)

        # financials
        rev = round(budget * (1 + np.random.normal(0.05,0.08)),2)
        financial_rows.append((pid, rev, "revenue", "USD", sdate))
        for _ in range(random.randint(1,4)):
            financial_rows.append((pid, round(abs(np.random.normal(2000,1500)),2), "expense", "USD", sdate + timedelta(days=random.randint(0,duration))))

    # Insert projects and project_stage
    cur.executemany("INSERT INTO project (project_id, project_name, description, start_date, end_date, client_id, team_id, budget, priority, current_stage_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", project_rows)
    cur.executemany("INSERT INTO project_stage (project_id, stage_id, start_date, end_date) VALUES (?, ?, ?, ?)", project_stage_rows)
    print(f"Inserted {len(project_rows)} projects and {len(project_stage_rows)} stages.")

    # update project.current_stage_id to last stage id for each project
    rows = cur.execute("SELECT id, project_id FROM project_stage ORDER BY id").fetchall()
    mapping = {}
    for r in rows:
        pid_r = r[1]
        mapping.setdefault(pid_r, []).append(r[0])
    for pid_k, lst in mapping.items():
        last_id = lst[-1]
        cur.execute("UPDATE project SET current_stage_id = ? WHERE project_id = ?", (last_id, pid_k))

    # insert team_member (remove duplicates)
    unique_tm = list({(tm[0],tm[1],tm[2],tm[3]) for tm in team_member_rows})
    cur.executemany("INSERT INTO team_member (team_id, employee_id, assigned_date, role_in_team) VALUES (?, ?, ?, ?)", unique_tm)
    print(f"Inserted {len(unique_tm)} team members")

    # Now insert issues, but assign project_stage_id by random stage of that project
    # fetch project_stage ids per project
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
    if final_issues:
        cur.executemany("INSERT INTO issue (project_id, project_stage_id, severity, concept, error_message, date_reported) VALUES (?, ?, ?, ?, ?, ?)", final_issues)
        print(f"Inserted {len(final_issues)} issues")

    # insert time_logs but need to compute cost by employee hourly
    # fetch employee hourly_cost
    emp_costs = {r[0]: float(r[6]) for r in cur.execute("SELECT employee_id, name, email, role, hire_date, capacity_weekly, hourly_cost FROM employee").fetchall()}
    final_tl = []
    for tl in time_log_rows:
        pid_t, eid_t, hrs, _, d = tl
        cost = round(hrs * emp_costs.get(eid_t, 30.0), 2)
        final_tl.append((pid_t, eid_t, hrs, cost, d))
    cur.executemany("INSERT INTO time_log (project_id, employee_id, hours, cost, log_date) VALUES (?, ?, ?, ?, ?)", final_tl)
    print(f"Inserted {len(final_tl)} time_log rows")

    # insert financials
    cur.executemany("INSERT INTO financials (project_id, amount, type, currency, date_recorded) VALUES (?, ?, ?, ?, ?)", financial_rows)
    print(f"Inserted {len(financial_rows)} financials")

    print("Generación finalizada.")

if __name__ == "__main__":
    generate()
