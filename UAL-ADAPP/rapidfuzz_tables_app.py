from db_fuzzy_match import connect_to_db, fuzzy_match, execute_dynamic_matching
import pandas as pd
import os

def display_results(results, formato="dataframe"):
    if formato == "dataframe":
        df = pd.DataFrame(results)
        print(df)
    elif formato == "dictionary":
        for record in results:
            print(record)
    else:
        print("formato invalido. Usa 'dataframe' o 'dictionary'.")

def export_results(results):
    if not results:
        print("No hay resultados para exportar. La exportación ha sido cancelada.")
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
                results = results[:max_rows]

            file_format = input("¿En qué formato deseas exportar los resultados? (csv/xlsx): ").strip().lower()
            filename = input("Introduce el nombre del archivo (con ruta opcional, sin extensión): ").strip()
            

            folder = os.path.dirname(filename)
            if folder:
                if not os.path.exists(folder):
                    os.makedirs(folder)
                    print(f"Carpeta creada: {folder}")
            else:
                folder = os.getcwd()

            df = pd.DataFrame(results)

            if file_format == "csv":
                if df.empty:
                    print("El archivo CSV no puede ser creado porque los resultados están vacíos.")
                    return
                df.to_csv(f"{filename}.csv", index=False, encoding="utf-8")
                print(f"Resultados exportados exitosamente a '{filename}.csv'.")
            elif file_format == "xlsx":
                if df.empty:
                    print("El archivo Excel no puede ser creado porque los resultados están vacíos.")
                    return
                df.to_excel(f"{filename}.xlsx", index=False, engine="openpyxl")
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
        "email": "email"
    }
}

resultados = execute_dynamic_matching(params_dict, score_cutoff=70)
formato = input("Elige formato de salida (dataframe/dictionary): ").strip().lower()
display_results(resultados, formato=formato)
export_results(resultados)