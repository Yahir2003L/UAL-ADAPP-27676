from rapidfuzz import process, fuzz
import mysql.connector
import pyodbc

def connect_to_db(params):
    db_type = params.get("db_type", "sqlserver")
    if db_type == "mysql":
        return mysql.connector.connect(
            host=params.get("server", "localhost"),
            user=params.get("username", ""),
            password=params.get("password", ""),
            database=params.get("database", "")
        )
    else:
        connection_string = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={params.get('server', '')};"
            f"DATABASE={params.get('database', '')};"
            f"UID={params.get('username', '')};"
            f"PWD={params.get('password', '')};"
            "Encrypt=yes;"
            "TrustServerCertificate=yes;"
        )
        return pyodbc.connect(connection_string)

def fuzzy_match(queryRecord, choices, score_cutoff=0):
    scorers = [fuzz.WRatio, fuzz.QRatio, fuzz.token_set_ratio, fuzz.ratio]
    processor = lambda x: str(x).lower()
    processed_query = processor(queryRecord)
    choices_data = []

    for choice in choices:
        dict_choices = dict(choice)
        queryMatch = ""
        dict_match_records = {}
        for k, v in dict_choices.items():
            if k != "DestRecordId":
                val = str(v) if v is not None else ""
                queryMatch += val
                dict_match_records[k] = v

        choices_data.append({
            'query_match': queryMatch,
            'dest_record_id': dict_choices.get('DestRecordId'),
            'match_record_values': dict_match_records
        })

    best_match = None
    best_score = score_cutoff

    for scorer in scorers:
        result = process.extractOne(
            query=processed_query,
            choices=[item['query_match'] for item in choices_data],
            scorer=scorer,
            score_cutoff=score_cutoff,
            processor=processor
        )

        if result:
            match_value, score, index = result
            if score >= best_score:
                matched_item = choices_data[index]
                best_match = {
                    'match_query': queryRecord,
                    'match_result': match_value,
                    'score': score,
                    'match_result_values': matched_item['match_record_values']
                }
        else:
            best_match = {
                'match_query': queryRecord,
                'match_result': None,
                'score': 0,
                'match_result_values': {}
            }
    return best_match

def execute_dynamic_matching(params_dict, score_cutoff=0):
    db_type = params_dict.get("db_type", "mysql")
    
    conn_source = connect_to_db({
        "db_type": db_type,
        "server": params_dict.get("server", ""),
        "username": params_dict.get("username", ""),
        "password": params_dict.get("password", ""),
        "database": params_dict.get("sourceDatabase")
    })

    conn_dest = connect_to_db({
        "db_type": db_type,
        "server": params_dict.get("server", ""),
        "username": params_dict.get("username", ""),
        "password": params_dict.get("password", ""),
        "database": params_dict.get("destDatabase")
    })

    cursor_src = conn_source.cursor()
    cursor_dest = conn_dest.cursor()

    if 'src_dest_mappings' not in params_dict or not params_dict['src_dest_mappings']:
        raise ValueError("Debe proporcionar src_dest_mappings con columnas origen y destino")

    src_cols = ", ".join(params_dict['src_dest_mappings'].keys())
    dest_cols = ", ".join(params_dict['src_dest_mappings'].values())

    sql_source = f"SELECT {src_cols} FROM {params_dict['sourceTable']}"
    sql_dest   = f"SELECT {dest_cols} FROM {params_dict['destTable']}"

    cursor_src.execute(sql_source)
    src_rows = cursor_src.fetchall()
    src_columns = [col[0] for col in cursor_src.description]
    source_data = [dict(zip(src_columns, row)) for row in src_rows]

    cursor_dest.execute(sql_dest)
    dest_rows = cursor_dest.fetchall()
    dest_columns = [col[0] for col in cursor_dest.description]
    dest_data = [dict(zip(dest_columns, row)) for row in dest_rows]

    conn_source.close()
    conn_dest.close()

    matching_records = []

    for record in source_data:
        dict_query_records = {}
        query = ""

        for src_col in params_dict['src_dest_mappings'].keys():
            val = record.get(src_col)
            query += str(val) if val is not None else ""
            dict_query_records[src_col] = val

        fm = fuzzy_match(query, dest_data, score_cutoff)
        dict_query_records.update(fm)
        dict_query_records.update({
            'sourceTable': params_dict['sourceTable'],
            'destTable': params_dict['destTable']
        })
        matching_records.append(dict_query_records)

    return matching_records


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
print(resultados)
