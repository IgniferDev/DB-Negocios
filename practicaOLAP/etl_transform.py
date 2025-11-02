# etl_transform.py
import pandas as pd

def transform_for_olap(csv_path):
    # Cargar datos extraídos
    df = pd.read_csv(csv_path)

    # Renombrar columnas clave
    df = df.rename(columns={
        "Clave": "ClaveMateria",
        "Materia": "NombreMateria",
        "Profesor": "Docente",
        "Salón": "Salon",
        "Nota": "Nota"
    })

    # Expandir días (ej. "L,M,V" → ["L", "M", "V"])
    df["Dias"] = df["Días"].str.split(",")
    df_expanded = df.explode("Dias")

    # Eliminar columnas innecesarias
    columnas_a_eliminar = [col for col in ["Carrera", "HoraInicio", "HoraFin", "Edificio", "Días"] if col in df_expanded.columns]
    df_expanded = df_expanded.drop(columns=columnas_a_eliminar)

    # Guardar resultado transformado
    df_expanded.to_csv("horarios_olap.csv", index=False)
    print("Transformación completa y guardada en horarios_olap.csv")

if __name__ == "__main__":
    transform_for_olap("horarios_raw.csv")
