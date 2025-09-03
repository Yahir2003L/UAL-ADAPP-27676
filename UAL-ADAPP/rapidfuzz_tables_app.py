from db_fuzzy_match import connect_to_db, fuzzy_match, execute_dynamic_matching
import pandas as pd
import os

def display_results(results, formato="dataframe"):
    if not results:
        print("No hay resultados para mostrar.")
        return

    df = pd.DataFrame(results)

    if "score" in df.columns:
        df["score"] = df["score"] * 100
        df["score"] = df["score"].round(2).astype(str) + "%"

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

def export_results(results, selected_df):
    if not results:
        print("No hay resultados para exportar. La exportación ha sido cancelada.")
        return

    if selected_df is None:
        print("No se seleccionaron columnas para exportar. Operación cancelada.")
        return

    export = input("¿Deseas exportar los resultados? (sí/no): ").strip().lower()
    if export in ["si", "s"]:
        try:
            limit_rows = input("¿Deseas limitar el número de filas exportadas? (sí/no): ").strip().lower()
            if limit_rows in ["si", "s"]:
                max_rows = int(input("Introduce el número máximo de filas a exportar: ").strip())
                if max_rows == 0:
                    print("El número máximo de filas no puede ser 0. La exportación ha sido cancelada.")
                    return
                selected_df = selected_df[:max_rows]

            file_format = input("¿En qué formato deseas exportar los resultados? (csv/xlsx): ").strip().lower()
            filename = input("Introduce el nombre del archivo (con ruta opcional, sin extensión): ").strip()
            
            folder = os.path.dirname(filename)
            if folder:
                if not os.path.exists(folder):
                    os.makedirs(folder)
                    print(f"Carpeta creada: {folder}")
            else:
                folder = os.getcwd()

            if file_format == "csv":
                if selected_df.empty:
                    print("El archivo CSV no puede ser creado porque los resultados están vacíos.")
                    return
                selected_df.to_csv(f"{filename}.csv", index=False, encoding="utf-8")
                print(f"Resultados exportados exitosamente a '{filename}.csv'.")
            elif file_format == "xlsx":
                if selected_df.empty:
                    print("El archivo Excel no puede ser creado porque los resultados están vacíos.")
                    return
                selected_df.to_excel(f"{filename}.xlsx", index=False, engine="openpyxl")
                print(f"Resultados exportados exitosamente a '{filename}.xlsx'.")
            else:
                print("Formato no válido. Usa 'csv' o 'xlsx'.")
        except ValueError:
            print("El número máximo de filas debe ser un número entero.")
        except Exception as e:
            print(f"Error al exportar los resultados: {e}")
    else:
        print("Exportación cancelada.")

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

resultados = execute_dynamic_matching(params_dict, score_cutoff=70)
formato = input("Elige formato de salida (dataframe/dictionary): ").strip().lower()
selected_df = display_results(resultados, formato=formato)
export_results(resultados, selected_df)