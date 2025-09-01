from db_fuzzy_match import connect_to_db, fuzzy_match, execute_dynamic_matching
import pandas as pd

def display_results(results, formato="dataframe"):
    if formato == "dataframe":
        df = pd.DataFrame(results)
        print(df)
    elif formato == "dictionary":
        for record in results:
            print(record)
    else:
        print("formato invalido. Usa 'dataframe' o 'dictionary'.")

def export_results_to_csv(results):
    export = input("¿Deseas exportar los resultados a un archivo CSV? (sí/no): ").strip().lower()
    if export in ["si", "s"]:
        filename = input("Introduce el nombre del archivo CSV (sin extensión): ").strip()
        df = pd.DataFrame(results)
        try:
            df.to_csv(f"{filename}.csv", index=False, encoding="utf-8")
            print(f"Resultados exportados exitosamente a '{filename}.csv'.")
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
export_results_to_csv(resultados)