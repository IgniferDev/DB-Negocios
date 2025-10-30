# etl_transform.py
import pandas as pd

def transform_for_olap(csv_path):
    df = pd.read_csv(csv_path)
    
    # Renombrar columnas clave
    df = df.rename(columns={
        "Clave": "ClaveMateria",
        "Materia": "NombreMateria",
        "Profesor": "Docente",
        "Salón": "Salon",
        "Nota": "Nota"  # Aseguramos que se mantenga
    })

    # Expandir días
    df["Dias"] = df["Días"].str.split(",")
    
    # Explode sin agregar columnas extra
    df_expanded = df.explode("Dias")
    
    # Eliminar columnas no deseadas si quedaron
    columnas_a_eliminar = [col for col in ["Carrera", "HoraInicio", "HoraFin", "Edificio"] if col in df_expanded.columns]
    df_expanded = df_expanded.drop(columns=columnas_a_eliminar)
    
    df_expanded.to_csv("horarios_olap.csv", index=False)
    print("Transformación completa y guardada en horarios_olap.csv")

if __name__ == "__main__":
    transform_for_olap("horarios_raw.csv")
