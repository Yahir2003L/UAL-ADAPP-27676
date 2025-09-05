import os
import pandas as pd
import sqlalchemy
import mysql.connector
from db_fuzzy_match import connect_to_db, fuzzy_match, execute_dynamic_matching


def display_results(results, formato="dataframe"):
    if not results:
        print("No hay resultados para mostrar.")
        return

    df = pd.DataFrame(results)

    if "score" in df.columns:
        if df["score"].max() <= 1.0:
            df["score"] = df["score"] * 100
        df["score"] = df["score"].round(2)

    if "nombre" in df.columns and "apellido" in df.columns:
        df["nombre_completo"] = df["nombre"] + " " + df["apellido"]

    print("Columnas disponibles:")
    for col in df.columns:
        print(f"- {col}")

    selected_columns = input("Introduce los nombres de las columnas que deseas imprimir, separados por comas: ").strip()

    if not selected_columns:
        print("No se ingresaron columnas. Operación cancelada.")
        return None

    selected_columns = [col.strip() for col in selected_columns.split(",")]

    if "score" not in selected_columns:
        selected_columns.append("score")

    invalid_columns = [col for col in selected_columns if col not in df.columns]
    if invalid_columns:
        print(f"Las siguientes columnas no existen: {', '.join(invalid_columns)}")
        print("Mostrando todas las columnas disponibles.")
        print(df)
        return df  
    else:
        try:
            selected_df = df[selected_columns]

            print("Renombrar columnas seleccionadas:")
            new_column_names = {}
            for col in selected_columns:
                new_name = input(f"Introduce el nuevo nombre para la columna '{col}' (o presiona enter para mantener el nombre actual): ").strip()
                if new_name:
                    new_column_names[col] = new_name

            selected_df = selected_df.rename(columns=new_column_names)
            print(selected_df)
            return selected_df
        except KeyError:
            print("Error al seleccionar las columnas. Verifica los nombres ingresados.")
            return df  


def separate_matched_records(df):
    if "score" not in df.columns:
        print("No se encontró la columna 'score'.")
        return None, None

    if df["score"].dtype == object and "%" in str(df["score"].iloc[0]):
        df["score"] = df["score"].str.replace("%", "").astype(float)

    matched = df[df["score"] >= 97]
    unmatched = df[df["score"] < 97]

    print(f"Registros coincidentes (score >= 97): {len(matched)}")
    print(f"Registros no coincidentes (score < 97): {len(unmatched)}")

    return matched, unmatched


def export_results_interactivo(df):
    if df is None or df.empty:
        print("No hay resultados para exportar. La exportación ha sido cancelada.")
        return

    export = input("¿Deseas exportar los resultados? (sí/no): ").strip().lower()
    if export not in ["si", "s"]:
        print("Exportación cancelada.")
        return

    try:
        limit_rows = input("¿Deseas limitar el número de filas exportadas? (sí/no): ").strip().lower()
        if limit_rows in ["si", "s"]:
            max_rows = int(input("Introduce el número máximo de filas a exportar: ").strip())
            if max_rows <= 0:
                print("El número máximo de filas debe ser mayor que 0. Cancelando exportación.")
                return
            df = df[:max_rows]

        file_format = input("¿En qué formato deseas exportar los resultados? (csv/xlsx): ").strip().lower()
        filename = input("Introduce el nombre del archivo (sin extensión): ").strip()

        folder = "exportaciones"
        os.makedirs(folder, exist_ok=True)
        file_path = os.path.join(folder, f"{filename}.{file_format}")

        if file_format == "csv":
            df.to_csv(file_path, index=False, encoding="utf-8")
        elif file_format == "xlsx":
            df.to_excel(file_path, index=False, engine="openpyxl")
        else:
            print("Formato no válido. Usa 'csv' o 'xlsx'.")
            return

        print(f"Resultados exportados exitosamente a '{file_path}'.")
    except ValueError:
        print("El número máximo de filas debe ser un número entero.")
    except Exception as e:
        print(f"Error al exportar los resultados: {e}")


def import_file_to_dataframe():
    filepath = input("Introduce la ruta del archivo a importar (CSV o Excel): ").strip()

    if not os.path.exists(filepath):
        print("El archivo no existe.")
        return None

    try:
        if filepath.endswith(".csv"):
            df = pd.read_csv(filepath)
        elif filepath.endswith(".xlsx"):
            df = pd.read_excel(filepath, engine="openpyxl")
        else:
            print("Formato de archivo no soportado.")
            return None

        print(f"Archivo importado correctamente con {len(df)} registros.")
        return df
    except Exception as e:
        print(f"Error al importar archivo: {e}")
        return None


def insert_dataframe_with_sp(df, db_params, proc_name="InsertMatchedRecord"):
    try:
        connection = mysql.connector.connect(
            host=db_params["server"],
            user=db_params["username"],
            password=db_params["password"],
            database=db_params["destDatabase"]
        )
        cursor = connection.cursor()

        for _, row in df.iterrows():
            args = (
                row.get("nombre"),
                row.get("apellido"),
                row.get("email"),
                row.get("match_query"),
                row.get("match_result"),
                float(row.get("score", 0)) if row.get("score") else 0,
                row.get("match_result_values"),
                row.get("destTable"),
                row.get("sourceTable")
            )
            cursor.callproc(proc_name, args)

        connection.commit()
        print(f"Se insertaron {len(df)} registros usando el procedimiento almacenado.")
    except Exception as e:
        print(f"Error al ejecutar procedimiento almacenado: {e}")
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()


# =====================
# MAIN
# =====================
params_dict = {
    "db_type": "mysql",
    "server": "localhost",
    "username": "root",
    "password": "",
    "sourceDatabase": "crm",
    "destDatabase": "dbo",
    "sourceTable": "Clientes",
    "destTable": "Usuarios",
    "src_dest_mappings": {
        "nombre": "first_name",
        "apellido": "last_name",
        "email": "email"
    }
}

columnas_fijas = ["nombre", "apellido", "email", "match_query", "match_result",  "score", "match_result_values", "destTable", "sourceTable"]
nombre_tabla_fija = "datos_importados"

resultados = execute_dynamic_matching(params_dict, score_cutoff=70)

formato = input("Elige formato de salida (dataframe/dictionary): ").strip().lower()
selected_df = display_results(resultados, formato=formato)

if selected_df is not None:
    matched_df, unmatched_df = separate_matched_records(selected_df)

    tipo_exportacion = input(
        "¿Qué resultados deseas exportar? (1: todos, 2: solo coincidentes, 3: solo no coincidentes, otro: cancelar): "
    ).strip()

    if tipo_exportacion == "1":
        export_results_interactivo(selected_df)
    elif tipo_exportacion == "2":
        export_results_interactivo(matched_df)
    elif tipo_exportacion == "3":
        export_results_interactivo(unmatched_df)
    else:
        print("Exportación cancelada.")

importar = input("¿Deseas importar un archivo y cargarlo a la base de datos? (sí/no): ").strip().lower()
if importar in ["si", "s"]:
    imported_df = import_file_to_dataframe()
    if imported_df is not None:
        insert_dataframe_with_sp(imported_df, params_dict)
