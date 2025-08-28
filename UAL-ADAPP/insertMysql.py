import csv
import mysql.connector
from datetime import datetime

def conectar_mysql(nombre_base):
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="",
        database=nombre_base
    )

def insertar_clientes(ruta_csv):
    conn = conectar_mysql("crm")
    cursor = conn.cursor()

    with open(ruta_csv, newline='', encoding='utf-8') as archivo:
        reader = csv.DictReader(archivo)
        for fila in reader:
            fecha = datetime.strptime(fila["fecha_registro"], "%d/%m/%Y %H:%M").strftime("%Y-%m-%d %H:%M:%S")
            cursor.execute("""
                INSERT INTO Clientes (cliente_id, nombre, apellido, email, FechaRegistro)
                VALUES (%s, %s, %s, %s, %s)
            """, (fila["cliente_id"], fila["nombre"], fila["apellido"], fila["email"], fecha))
    
    conn.commit()
    cursor.close()
    conn.close()
    print("Clientes insertados en 'crm'.")

def insertar_usuarios(ruta_csv):
    conn = conectar_mysql("dbo")
    cursor = conn.cursor()

    with open(ruta_csv, newline='', encoding='utf-8') as archivo:
        reader = csv.DictReader(archivo)
        for fila in reader:
            fecha = datetime.strptime(fila["fecha_creacion"], "%d/%m/%Y %H:%M").strftime("%Y-%m-%d %H:%M:%S")
            cursor.execute("""
                INSERT INTO Usuarios (userId, username, first_name, last_name, email, password_hash, rol, fecha_creacion)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                fila["userId"], fila["username"], fila["first_name"], fila["last_name"],
                fila["email"], fila["password_hash"], fila["rol"], fecha
            ))

    conn.commit()
    cursor.close()
    conn.close()
    print("Usuarios insertados en 'dbo'.")

insertar_clientes("clientes.csv")
insertar_usuarios("usuarios.csv")
