# etl_extract.py
import pdfplumber
import pandas as pd
import glob

def extract_tables_from_pdfs(pdf_folder):
    all_data = []
    for pdf_file in glob.glob(r"C:\Users\USER\Desktop\Negocios\practicaOLAP\pdf_datos\*.pdf"):
        with pdfplumber.open(pdf_file) as pdf:
            for page in pdf.pages:
                table = page.extract_table()
                if table:
                    df = pd.DataFrame(table[1:], columns=table[0])
                    
                    # Renombrar columna sin nombre a "Nota"
                    df.columns = [col if col else "Nota" for col in df.columns]
                    df.columns = [col if not col.startswith("Unnamed") else "Nota" for col in df.columns]
                    
                    all_data.append(df)
    return pd.concat(all_data, ignore_index=True)

if __name__ == "__main__":
    df = extract_tables_from_pdfs("pdf_datos")
    df.to_csv("horarios_raw.csv", index=False)
    print("Datos extraídos y guardados en horarios_raw.csv")
