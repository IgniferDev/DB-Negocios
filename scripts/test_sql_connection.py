import pyodbc
def test_connection(server, database, username, password):
    try:
        conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}"
        conn = pyodbc.connect(conn_str, timeout=5)
        cursor = conn.cursor()
        cursor.execute("SELECT DB_NAME(), @@SERVERNAME;")
        print(f"✅ Conectado a {server}")
        for row in cursor.fetchall():
            print(row)
        conn.close()
    except Exception as e:
        print(f"❌ Error al conectar a {server}: {e}")
# Base de datos 1 (Laptop A)
test_connection("172.24.56.35,1433", "PROJECT_MANAGE", "Inteligencia", "Rock2213#")
# Base de datos 2 (Laptop B)
test_connection("172.24.84.67,1433", "PROJECT_SUPPORT_SYSTEM", "admin_kiry2", "1234")


