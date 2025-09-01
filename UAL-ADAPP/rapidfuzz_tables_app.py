from db_fuzzy_match import connect_to_db, fuzzy_match, execute_dynamic_matching
import pandas as pd

def display_results(results, output_format="dataframe"):
    if output_format == "dataframe":
        df = pd.DataFrame(results)
        print(df)
    elif output_format == "dictionary":
        for record in results:
            print(record)
    else:
        print("formato invalido. Usa 'dataframe' o 'dictionary'.")
    

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
output_format = input("Elige formato de salida (dataframe/dictionary): ").strip().lower()
display_results(resultados, output_format= output_format)