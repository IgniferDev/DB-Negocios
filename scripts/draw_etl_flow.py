# draw_etl_flow.py
from graphviz import Digraph
import os

dot = Digraph('ETL_Flow', format='png')
dot.attr(rankdir='LR', fontsize='10')

# nodes
dot.node('SRC', 'SOURCE\nPROJECT_MANAGE\n(Ponki)\nTables: employee, client, team, project, time_log, financials, issue', shape='folder', style='filled', fillcolor='#EFEFEF')
dot.node('ETL', 'ETL (Ignifer)\nPython: pyodbc / SQLAlchemy / pandas', shape='box', style='filled', fillcolor='#FFF2CC')
dot.node('TRANS', 'TRANSFORM\nAggregations & Cleans', shape='box3d', style='filled', fillcolor='#D9EAD3')
dot.node('DIM', 'DIMENSIONS\nupsert via MERGE\n(dim_client, dim_team, dim_project, dim_date)', shape='cylinder', style='filled', fillcolor='#CFE2F3')
dot.node('FACT', 'FACTS\nfact_project\n(DELETE+INSERT)', shape='cylinder', style='filled', fillcolor='#F4CCCC')
dot.node('AUDIT', 'AUDIT\netl_audit', shape='note', style='filled', fillcolor='#D9D2E9')
dot.node('DST', 'DW\nPROJECT_SUPPORT_SYSTEM\n(Kiry)', shape='folder', style='filled', fillcolor='#EFEFEF')

# connections
dot.edge('SRC', 'ETL', label='extract\n(pandas.read_sql / pyodbc)')
dot.edge('ETL', 'TRANS', label='transform\n(agg: hours, cost, revenue, errors)')
dot.edge('TRANS', 'DIM', label='upsert dims\nMERGE via #tmp_*')
dot.edge('TRANS', 'FACT', label='build df_fact\nrows = [(project_id, client_id, ...)]')
dot.edge('FACT', 'DST', label='load facts\nexecutemany (fast_executemany)')
dot.edge('DIM', 'DST', label='load dims\nMERGE')
dot.edge('ETL', 'AUDIT', label='record run\ninsert etl_audit')

# extra notes
dot.attr(label='Diagrama de flujo ETL — Extract → Transform → Load\nCredenciales en .env (python-dotenv). Date range ±30d for dim_date.', fontsize='9')

out_path = 'etl_flow'
dot.render(out_path, view=False)   # produces etl_flow.png and etl_flow (source .gv)
print("Generated:", out_path + '.png')
