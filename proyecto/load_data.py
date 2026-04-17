import json
import hashlib
from datetime import datetime
import mysql.connector
from pymongo import MongoClient
from pymongo.errors import BulkWriteError
import configuracion # Importa tus rutas y credenciales

def inicializar_bases_de_datos():
    """Crea la base de datos y las tablas en MySQL si no existen."""
    print("1. Preparando infraestructura de bases de datos...")
    try:
        # Conexión inicial para crear la base de datos
        config_temp = configuracion.MYSQL_CONFIG.copy()
        db_name = config_temp.pop('database')
        
        conexion = mysql.connector.connect(**config_temp)
        cursor = conexion.cursor()
        
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
        cursor.execute(f"USE {db_name}")

        # Definición de tablas (Diseño Normalizado)
        tablas = [
            "CREATE TABLE IF NOT EXISTS Categorias (id_categoria INT AUTO_INCREMENT PRIMARY KEY, nombre_categoria VARCHAR(100) NOT NULL UNIQUE)",
            "CREATE TABLE IF NOT EXISTS Usuarios (reviewerID VARCHAR(50) PRIMARY KEY, reviewerName VARCHAR(255))",
            "CREATE TABLE IF NOT EXISTS Productos (asin VARCHAR(50) PRIMARY KEY, id_categoria INT, FOREIGN KEY (id_categoria) REFERENCES Categorias(id_categoria))",
            "CREATE TABLE IF NOT EXISTS Reviews_Core (id_review VARCHAR(64) PRIMARY KEY, reviewerID VARCHAR(50), asin VARCHAR(50), overall INT, unixReviewTime BIGINT, fecha_formateada DATE, FOREIGN KEY (reviewerID) REFERENCES Usuarios(reviewerID), FOREIGN KEY (asin) REFERENCES Productos(asin))"
        ]
        
        for tabla in tablas:
            cursor.execute(tabla)
        
        conexion.commit()
        cursor.close()
        conexion.close()
        print("MySQL: Base de datos y tablas verificadas.")
        
    except Exception as e:
        print(f"Error crítico en MySQL: {e}")
        exit() # Si no hay tablas, no podemos seguir

def cargar_ficheros():
    """Lee los JSON línea a línea e inserta por lotes (Batching) evitando duplicados."""
    print("2. Iniciando conexiones para carga de datos...")
    
    # Conexión MySQL
    conn_mysql = mysql.connector.connect(**configuracion.MYSQL_CONFIG)
    cursor = conn_mysql.cursor()
    
    # Conexión MongoDB
    client_mongo = MongoClient(configuracion.MONGO_CONFIG["uri"])
    db_mongo = client_mongo[configuracion.MONGO_CONFIG["database"]]
    col_mongo = db_mongo[configuracion.MONGO_CONFIG["collection"]]

    # Tamaño del lote para eficiencia de memoria y red 
    BATCH_SIZE = 2000 

    for nombre_cat, ruta in configuracion.RUTAS_JSON.items():
        print(f"\nProcesando categoría: {nombre_cat}")
        
        # Asegurar que la categoría existe y obtener su ID
        cursor.execute("INSERT IGNORE INTO Categorias (nombre_categoria) VALUES (%s)", (nombre_cat,))
        conn_mysql.commit()
        cursor.execute("SELECT id_categoria FROM Categorias WHERE nombre_categoria = %s", (nombre_cat,))
        id_cat = cursor.fetchone()[0]

        # Listas temporales para el Batching
        batch_usuarios, batch_productos, batch_reviews_sql, batch_reviews_mongo = [], [], [], []

        try:
            with open(ruta, 'r', encoding='utf-8') as f:
                for num_linea, linea in enumerate(f, 1):
                    data = json.loads(linea.strip()) # [cite: 13]

                    # 1. Generar ID único para enlazar MySQL y MongoDB
                    token = f"{data['reviewerID']}_{data['asin']}_{data['unixReviewTime']}"
                    id_review = hashlib.sha256(token.encode()).hexdigest()

                    # 2. Parsear fecha (mes día, año -> YYYY-MM-DD) 
                    fecha_dt = datetime.strptime(data['reviewTime'], '%m %d, %Y')
                    fecha_sql = fecha_dt.strftime('%Y-%m-%d')

                    # 3. Preparar datos MySQL
                    batch_usuarios.append((data['reviewerID'], data.get('reviewerName', 'Anónimo')))
                    batch_productos.append((data['asin'], id_cat))
                    batch_reviews_sql.append((id_review, data['reviewerID'], data['asin'], data['overall'], data['unixReviewTime'], fecha_sql))

                    # 4. Preparar datos MongoDB (Textos y arrays) [cite: 17, 18, 20]
                    batch_reviews_mongo.append({
                        "_id": id_review,
                        "helpful": data['helpful'],
                        "reviewText": data['reviewText'],
                        "summary": data['summary']
                    })

                    # 5. Ejecutar inserción al completar el tamaño del lote
                    if len(batch_reviews_sql) >= BATCH_SIZE:
                        # MySQL: INSERT IGNORE evita errores por duplicados [cite: 58]
                        cursor.executemany("INSERT IGNORE INTO Usuarios (reviewerID, reviewerName) VALUES (%s, %s)", batch_usuarios)
                        cursor.executemany("INSERT IGNORE INTO Productos (asin, id_categoria) VALUES (%s, %s)", batch_productos)
                        cursor.executemany("INSERT IGNORE INTO Reviews_Core VALUES (%s, %s, %s, %s, %s, %s)", batch_reviews_sql)
                        
                        # MongoDB: ordered=False permite que si un ID falla, el resto siga
                        try:
                            col_mongo.insert_many(batch_reviews_mongo, ordered=False)
                        except BulkWriteError:
                            pass # Ignoramos duplicados en MongoDB
                        
                        conn_mysql.commit()
                        print(f"   -> {num_linea} registros procesados...", end='\r')
                        batch_usuarios, batch_productos, batch_reviews_sql, batch_reviews_mongo = [], [], [], []

            # 6. Insertar registros restantes del último lote
            if batch_reviews_sql:
                cursor.executemany("INSERT IGNORE INTO Usuarios (reviewerID, reviewerName) VALUES (%s, %s)", batch_usuarios)
                cursor.executemany("INSERT IGNORE INTO Productos (asin, id_categoria) VALUES (%s, %s)", batch_productos)
                cursor.executemany("INSERT IGNORE INTO Reviews_Core VALUES (%s, %s, %s, %s, %s, %s)", batch_reviews_sql)
                try:
                    col_mongo.insert_many(batch_reviews_mongo, ordered=False)
                except BulkWriteError:
                    pass
                conn_mysql.commit()

            print(f"Categoría {nombre_cat} cargada con éxito.")

        except FileNotFoundError:
            print(f"Archivo no encontrado: {ruta}. Saltando...")
        except Exception as e:
            print(f"Error procesando {nombre_cat}: {e}")
        
    cursor.close()
    conn_mysql.close()
    client_mongo.close()
    print("\nPROCESO DE CARGA FINALIZADO.")

if __name__ == "__main__":
    print("=== PROYECTO BASES DE DATOS - CARGA DE DATOS ===")
    inicializar_bases_de_datos()
    cargar_ficheros()