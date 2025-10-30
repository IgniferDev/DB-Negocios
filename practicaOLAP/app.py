from flask import Flask, render_template, request
import pandas as pd

app = Flask(__name__)
df = pd.read_csv("horarios_olap.csv")

@app.route("/", methods=["GET", "POST"])
def index():
    result = None
    mensaje = ""
    if request.method == "POST":
        if "buscar_docente" in request.form:
            docente = request.form.get("docente", "").strip()
            if docente:
                result = df[df["Docente"].str.contains(docente, case=False)]
            else:
                mensaje = "Por favor ingresa el nombre del docente."
        elif "buscar_materia" in request.form:
            materia = request.form.get("materia", "").strip()
            if materia:
                result = df[df["NombreMateria"].str.contains(materia, case=False)][["Docente", "NombreMateria"]].drop_duplicates()
            else:
                mensaje = "Por favor ingresa el nombre de la materia."
        elif "buscar_nrc" in request.form:
            nrc = request.form.get("nrc", "").strip()
            if nrc:
                result = df[df["NRC"].astype(str).str.contains(nrc)]
            else:
                mensaje = "Por favor ingresa el NRC."
        elif "buscar_clave" in request.form:
            clave = request.form.get("clave", "").strip()
            if clave:
                result = df[df["ClaveMateria"].astype(str).str.contains(clave)]
            else:
                mensaje = "Por favor ingresa la clave de materia."
        elif "mostrar_todo" in request.form:
            result = df.copy()
    return render_template("index.html", result=result, mensaje=mensaje)

if __name__ == "__main__":
    app.run(debug=True)
