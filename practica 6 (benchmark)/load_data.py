import json
import mysql.connector
from pymongo import MongoClient
import sys

RUTA_ARCHIVO = r"C:\Users\lopez\Downloads\Video_Games_5.json"

MYSQL_HOST = "localhost"
MYSQL_USER = "root"
MYSQL_PASS = "admin"
MYSQL_DB = "AmazonReviews"

MONGO_URI = "mongodb://localhost:27017/"


def inserta_mongodb():
    """lee el fichero de reviews e inserta los json en mongodb."""
    print("iniciando carga en mongodb")

    try:
        cliente = MongoClient(MONGO_URI)
        db = cliente["Reviews"]
        coleccion = db["Videogames"]

        coleccion.drop()

        buffer_json = []
        contador = 0

        with open(RUTA_ARCHIVO, "r", encoding="utf-8") as f:
            for linea in f:
                doc = json.loads(linea)
                buffer_json.append(doc)
                contador += 1

                if len(buffer_json) == 10000:
                    coleccion.insert_many(buffer_json)
                    buffer_json = []
                    print(f"mongo: {contador} reviews insertadas")

            if buffer_json:
                coleccion.insert_many(buffer_json)

        print(f"mongodb ha cargado {contador} reviews totales\n")
    except Exception as e:
        print(f"error en mongodb: {e}")


def inserta_datos_mysql():
    """lee el fichero json y adapta los datos para insertarlos en mysql."""
    print("iniciando carga en mysql")

    try:
        conexion = mysql.connector.connect(
            host=MYSQL_HOST, user=MYSQL_USER, password=MYSQL_PASS
        )
        cursor = conexion.cursor()

        cursor.execute(f"DROP DATABASE IF EXISTS {MYSQL_DB}")
        cursor.execute(
            f"CREATE DATABASE {MYSQL_DB} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
        )
        cursor.execute(f"USE {MYSQL_DB}")

        tabla_sql = """
            CREATE TABLE reviews (
                id INT AUTO_INCREMENT PRIMARY KEY,
                reviewerID VARCHAR(255),
                asin VARCHAR(255),
                reviewerName VARCHAR(255),
                helpful_1 INT,
                helpful_2 INT,
                reviewText TEXT,
                overall FLOAT,
                summary TEXT,
                unixReviewTime BIGINT,
                reviewTime VARCHAR(50)
            )
        """
        cursor.execute(tabla_sql)

        sql_insert = """
            INSERT INTO reviews (reviewerID, asin, reviewerName, helpful_1, helpful_2,
                                 reviewText, overall, summary, unixReviewTime, reviewTime)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        buffer_tuplas = []
        contador = 0

        with open(RUTA_ARCHIVO, "r", encoding="utf-8") as f:
            for linea in f:
                doc = json.loads(linea)

                helpful_array = doc.get("helpful", [0, 0])
                h1 = helpful_array[0] if len(helpful_array) > 0 else 0
                h2 = helpful_array[1] if len(helpful_array) > 1 else 0

                rev_name = doc.get("reviewerName", "")
                if rev_name and len(rev_name) > 255:
                    rev_name = rev_name[:255]

                tupla = (
                    doc.get("reviewerID", ""),
                    doc.get("asin", ""),
                    rev_name,
                    h1,
                    h2,
                    doc.get("reviewText", ""),
                    doc.get("overall", 0.0),
                    doc.get("summary", ""),
                    doc.get("unixReviewTime", 0),
                    doc.get("reviewTime", ""),
                )
                buffer_tuplas.append(tupla)
                contador += 1

                if len(buffer_tuplas) == 10000:
                    cursor.executemany(sql_insert, buffer_tuplas)
                    buffer_tuplas = []
                    print(f"mysql: {contador} reviews procesadas")

            if buffer_tuplas:
                cursor.executemany(sql_insert, buffer_tuplas)

        conexion.commit()
        print(f"mysql ha cargado {contador} reviews totales")

        cursor.close()
        conexion.close()

    except FileNotFoundError:
        print(f"error: no se encuentra el archivo en {RUTA_ARCHIVO}")
        sys.exit(1)
    except Exception as e:
        print(f"error en mysql: {e}")


if __name__ == "__main__":
    inserta_mongodb()
    inserta_datos_mysql()
    print("\nproceso de carga completado")
