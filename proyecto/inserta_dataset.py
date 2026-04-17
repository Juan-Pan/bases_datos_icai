import json
import hashlib
from datetime import datetime
import mysql.connector
from pymongo import MongoClient
import configuracion

def insertar_nuevo_dataset():
    print("INSERTAR NUEVO DATASET")
    ruta = input("Introduce la ruta del nuevo archivo JSON (ej. ./datos/Kindle_Store_5.json): ")
    nombre_cat = input("Introduce el nombre de esta nueva categoría (ej. Kindle_Store): ")

    try:
        conn_mysql = mysql.connector.connect(**configuracion.MYSQL_CONFIG)
        cursor = conn_mysql.cursor()
        
        client_mongo = MongoClient(configuracion.MONGO_CONFIG["uri"])
        db_mongo = client_mongo[configuracion.MONGO_CONFIG["database"]]
        col_mongo = db_mongo[configuracion.MONGO_CONFIG["collection"]]

        # 1. Crear la nueva categoría
        cursor.execute("INSERT IGNORE INTO Categorias (nombre_categoria) VALUES (%s)", (nombre_cat,))
        conn_mysql.commit()
        cursor.execute("SELECT id_categoria FROM Categorias WHERE nombre_categoria = %s", (nombre_cat,))
        id_cat = cursor.fetchone()[0]

        print("Procesando archivo (esto puede tardar unos minutos)...")
        with open(ruta, 'r', encoding='utf-8') as f:
            for num_linea, linea in enumerate(f, 1):
                data = json.loads(linea.strip())

                # Generar ID
                token = f"{data['reviewerID']}_{data['asin']}_{data['unixReviewTime']}"
                id_review = hashlib.sha256(token.encode()).hexdigest()

                # Fecha
                fecha_sql = datetime.strptime(data['reviewTime'], '%m %d, %Y').strftime('%Y-%m-%d')

                # Insertar en MySQL fila a fila (como es un script auxiliar, no hacemos batching para simplificar el código)
                cursor.execute("INSERT IGNORE INTO Usuarios (reviewerID, reviewerName) VALUES (%s, %s)", (data['reviewerID'], data.get('reviewerName', 'Anónimo')))
                cursor.execute("INSERT IGNORE INTO Productos (asin, id_categoria) VALUES (%s, %s)", (data['asin'], id_cat))
                cursor.execute("INSERT IGNORE INTO Reviews_Core VALUES (%s, %s, %s, %s, %s, %s)", 
                               (id_review, data['reviewerID'], data['asin'], data['overall'], data['unixReviewTime'], fecha_sql))
                
                # Insertar en MongoDB
                try:
                    col_mongo.insert_one({
                        "_id": id_review,
                        "helpful": data['helpful'],
                        "reviewText": data['reviewText'],
                        "summary": data['summary']
                    })
                except Exception:
                    pass # Ignorar duplicados

                if num_linea % 1000 == 0:
                    print(f"   -> {num_linea} líneas insertadas...", end='\r')
                    conn_mysql.commit()

        conn_mysql.commit()
        print(f"\nNuevo dataset '{nombre_cat}' cargado exitosamente en el sistema híbrido.")
        
    except FileNotFoundError:
        print("Error: Archivo no encontrado. Revisa la ruta.")
    finally:
        cursor.close()
        conn_mysql.close()
        client_mongo.close()

if __name__ == "__main__":
    insertar_nuevo_dataset()