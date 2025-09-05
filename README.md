Conecta a una base de datos Azure SQL.

Parámetros:

server (str): Dirección del servidor SQL.
database (str): Nombre de la base de datos.
username (str): Usuario para la conexión.
password (str): Contraseña para la conexión.

Retorno:

pyodbc.Connection: Objeto de conexión a la base de datos.
fuzzy_match(queryRecord, choices, score_cutoff=0)
Realiza una coincidencia difusa entre un registro de consulta y una lista de opciones.

Parámetros:

queryRecord (str): Registro de consulta a buscar.
choices (list[dict]): Lista de opciones donde buscar coincidencias.
score_cutoff (int, opcional): Puntuación mínima para considerar una coincidencia. Por defecto, 0.

Retorno:

dict: Mejor coincidencia encontrada con los campos:
match_query (str): Consulta original.
match_result (str): Resultado coincidente.
score (int): Puntuación de la coincidencia.
match_result_values (dict): Valores del registro coincidente.
execute_dynamic_matching(params_dict, score_cutoff=0)

Ejecuta un proceso de coincidencia difusa entre tablas de origen y destino.

Parámetros:

params_dict (dict): Diccionario con los parámetros de configuración:
server (str): Dirección del servidor SQL.
database (str): Nombre de la base de datos.
username (str): Usuario para la conexión.
password (str): Contraseña para la conexión.
sourceSchema (str): Esquema de la tabla de origen.
sourceTable (str): Nombre de la tabla de origen.
destSchema (str): Esquema de la tabla de destino.
destTable (str): Nombre de la tabla de destino.
src_dest_mappings (dict): Mapeo de columnas entre origen y destino.
score_cutoff (int, opcional): Puntuación mínima para considerar una coincidencia. Por defecto, 0.

Retorno:

list[dict]: Lista de registros con los resultados de las coincidencias.
Cambios realizados

Centralización de configuración en conectar_mysql
Se implementó un diccionario de configuración (config) para evitar repetir los parámetros de conexión en cada llamada. Esto facilita la mantenibilidad del código.

Uso de executemany en inserciones (Semana 1)
Se sustituyó el uso de cursor.execute() dentro de un bucle por cursor.executemany() para insertar múltiples filas en una sola operación, optimizando el rendimiento en la carga de datos.

Migración a procedimientos almacenados (Semana 2)
Ahora todas las inserciones se realizan mediante Stored Procedures en MySQL.
Esto mejora el rendimiento y la seguridad, delegando la lógica de inserción al motor de base de datos.

Se aplica la convención de nombres:

sp_[process]_[source]_[target]_[numero de control]


Ejemplo:

sp_insert_Clientes_Usuarios_01


Actualización de funciones de inserción en Python
Todas las funciones que insertaban datos ahora usan:

cursor.callproc("sp_insert_Clientes_Usuarios_01", args)


Repositorio actualizado con scripts SQL

Los procedimientos almacenados se encuentran en la carpeta /sql/.

