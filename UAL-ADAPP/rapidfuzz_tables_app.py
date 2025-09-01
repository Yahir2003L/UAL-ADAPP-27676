from db_fuzzy_match import connect_to_db, fuzzy_match, execute_dynamic_matching
import pandas as pd

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
df = pd.DataFrame(resultados)
print(df)