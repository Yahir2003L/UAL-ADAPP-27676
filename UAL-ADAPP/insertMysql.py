import csv
import mysql.connector
from datetime import datetime

def conectar_mysql(nombre_base):
    # Cambio 1: reutilizar el diccionario de configuración en lugar de repetir parámetros
    config = {
        "host": "localhost",
        "user": "root",
        "password": "",
        "database": nombre_base
    }
    return mysql.connector.connect(**config)

def insertar_clientes(ruta_csv):
    conn = conectar_mysql("crm")
    cursor = conn.cursor()

    with open(ruta_csv, newline='', encoding='utf-8') as archivo:
        reader = csv.DictReader(archivo)

        # Cambio 2: usar executemany para insertar varias filas en una sola llamada, en lugar de ejecutar INSERT fila por fila
        datos = []
        for fila in reader:
            fecha = datetime.strptime(fila["fecha_registro"], "%d/%m/%Y %H:%M").strftime("%Y-%m-%d %H:%M:%S")
            datos.append((fila["cliente_id"], fila["nombre"], fila["apellido"], fila["email"], fecha))

        cursor.executemany("""
            INSERT INTO Clientes (cliente_id, nombre, apellido, email, FechaRegistro)
            VALUES (%s, %s, %s, %s, %s)
        """, datos)
    
    conn.commit()
    cursor.close()
    conn.close()
    print("Clientes insertados en 'crm'.")

def insertar_usuarios(ruta_csv):
    conn = conectar_mysql("dbo")
    cursor = conn.cursor()

    with open(ruta_csv, newline='', encoding='utf-8') as archivo:
        reader = csv.DictReader(archivo)
        datos = []
        for fila in reader:
            fecha = datetime.strptime(fila["fecha_creacion"], "%d/%m/%Y %H:%M").strftime("%Y-%m-%d %H:%M:%S")
            datos.append((
                fila["userId"], fila["username"], fila["first_name"], fila["last_name"],
                fila["email"], fila["password_hash"], fila["rol"], fecha
            ))

        cursor.executemany("""
            INSERT INTO Usuarios (userId, username, first_name, last_name, email, password_hash, rol, fecha_creacion)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, datos)

    conn.commit()
    cursor.close()
    conn.close()
    print("Usuarios insertados en 'dbo'.")

insertar_clientes("clientes.csv")
insertar_usuarios("usuarios.csv")
