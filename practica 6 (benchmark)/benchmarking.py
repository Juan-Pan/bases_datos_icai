import time
import mysql.connector
import matplotlib.pyplot as plt
import numpy as np
from pymongo import MongoClient
import sys

MYSQL_HOST = "localhost"
MYSQL_USER = "root"
MYSQL_PASS = "admin"
MYSQL_DB = "AmazonReviews"

MONGO_URI = "mongodb://localhost:27017/"

try:
    conexion_mysql = mysql.connector.connect(
        host=MYSQL_HOST, user=MYSQL_USER, password=MYSQL_PASS, database=MYSQL_DB
    )
    cursor = conexion_mysql.cursor()
except Exception as e:
    print(f"Error conectando a MySQL: {e}")
    sys.exit(1)

try:
    cliente_mongo = MongoClient(MONGO_URI)
    coleccion_mongo = cliente_mongo["Reviews"]["Videogames"]
except Exception as e:
    print(f"Error conectando a MongoDB: {e}")
    sys.exit(1)


def Benchmark_1():
    print("\nbenchmark1: usuarios distintos")

    sql_query = "SELECT DISTINCT reviewerID FROM reviews"
    registros_mysql = 0
    inicio_mysql = time.time()
    for _ in range(5):
        cursor.execute(sql_query)
        resultados = cursor.fetchall()
        registros_mysql = len(resultados)
    fin_mysql = time.time()
    tiempo_mysql = fin_mysql - inicio_mysql
    print(f"registros recuperados mysql: {registros_mysql}")
    print(f"tiempo obtenido en encontrar usuarios distintos mysql (x5): {tiempo_mysql:.6f}")

    registros_mongo = 0
    inicio_mongo = time.time()
    for _ in range(5):
        resultados_mongo = coleccion_mongo.distinct("reviewerID")
        registros_mongo = len(resultados_mongo)
    fin_mongo = time.time()
    tiempo_mongo = fin_mongo - inicio_mongo
    print(f"registros recuperados mongodb: {registros_mongo}")
    print(f"tiempo obtenido en encontrar usuarios distintos mongodb (x5): {tiempo_mongo:.6f}")

    return tiempo_mysql, tiempo_mongo


def Benchmark_2():
    print("\nbenchmark2: media de overall por cada usuario")

    sql_query = "SELECT reviewerID, AVG(overall) as media_notas, COUNT(*) as total_reviews FROM reviews GROUP BY reviewerID"
    registros_mysql = 0
    inicio_mysql = time.time()
    for _ in range(5):
        cursor.execute(sql_query)
        resultados = cursor.fetchall()
        registros_mysql = len(resultados)
    fin_mysql = time.time()
    tiempo_mysql = fin_mysql - inicio_mysql
    print(f"registros recuperados mysql: {registros_mysql}")
    print(f"tiempo obtenido en media de overall mysql (x5): {tiempo_mysql:.6f}")

    pipeline = [
        {"$group": {"_id": "$reviewerID", "media_notas": {"$avg": "$overall"}, "total_reviews": {"$sum": 1}}}
    ]
    registros_mongo = 0
    inicio_mongo = time.time()
    for _ in range(5):
        resultados_mongo = list(coleccion_mongo.aggregate(pipeline))
        registros_mongo = len(resultados_mongo)
    fin_mongo = time.time()
    tiempo_mongo = fin_mongo - inicio_mongo
    print(f"registros recuperados mongodb: {registros_mongo}")
    print(f"tiempo obtenido en media de overall mongodb (x5): {tiempo_mongo:.6f}")

    return tiempo_mysql, tiempo_mongo


def Benchmark_3():
    print("\nbenchmark3: encontrar la palabra great")

    sql_query = "SELECT id FROM reviews WHERE summary LIKE '%great%'"
    registros_mysql = 0
    inicio_mysql = time.time()
    for _ in range(5):
        cursor.execute(sql_query)
        resultados = cursor.fetchall()
        registros_mysql = len(resultados)
    fin_mysql = time.time()
    tiempo_mysql = fin_mysql - inicio_mysql
    print(f"registros recuperados mysql: {registros_mysql}")
    print(f"tiempo obtenido en encontrar texto en summary que diga great mysql (x5): {tiempo_mysql:.6f}")

    query_mongo = {"summary": {"$regex": "great", "$options": "i"}}
    registros_mongo = 0
    inicio_mongo = time.time()
    for _ in range(5):
        resultados_mongo = list(coleccion_mongo.find(query_mongo))
        registros_mongo = len(resultados_mongo)
    fin_mongo = time.time()
    tiempo_mongo = fin_mongo - inicio_mongo
    print(f"registros recuperados mongodb: {registros_mongo}")
    print(f"tiempo obtenido en encontrar texto en summary que diga great mongodb (x5): {tiempo_mongo:.6f}")

    return tiempo_mysql, tiempo_mongo


def Benchmark_4():
    print("\nbenchmark4: item con la media mas alta")

    sql_query = "SELECT asin FROM reviews GROUP BY asin ORDER BY AVG(overall) DESC, asin ASC LIMIT 1"
    registros_mysql = 0
    inicio_mysql = time.time()
    for _ in range(5):
        cursor.execute(sql_query)
        resultados = cursor.fetchall()
        registros_mysql = len(resultados)
    fin_mysql = time.time()
    tiempo_mysql = fin_mysql - inicio_mysql
    print(f"registros recuperados mysql: {registros_mysql}")
    print(f"tiempo obtenido en mostrar el item con la media mas alta (x5): {tiempo_mysql:.6f}")

    pipeline = [
        {"$group": {"_id": "$asin", "media_notas": {"$avg": "$overall"}}},
        {"$sort": {"media_notas": -1, "_id": 1}},
        {"$limit": 1}
    ]
    registros_mongo = 0
    inicio_mongo = time.time()
    for _ in range(5):
        resultados_mongo = list(coleccion_mongo.aggregate(pipeline))
        registros_mongo = len(resultados_mongo)
    fin_mongo = time.time()
    tiempo_mongo = fin_mongo - inicio_mongo
    print(f"registros recuperados mongodb: {registros_mongo}")
    print(f"tiempo obtenido en mostrar el item con la media mas alta (x5): {tiempo_mongo:.6f}")

    return tiempo_mysql, tiempo_mongo


def Benchmark_5():
    print("\nbenchmark5: item con la media mas alta (al menos 10 reviews)")

    sql_query = "SELECT asin FROM reviews GROUP BY asin HAVING COUNT(*) >= 10 ORDER BY AVG(overall) DESC, asin ASC LIMIT 1"
    registros_mysql = 0
    inicio_mysql = time.time()
    for _ in range(5):
        cursor.execute(sql_query)
        resultados = cursor.fetchall()
        registros_mysql = len(resultados)
    fin_mysql = time.time()
    tiempo_mysql = fin_mysql - inicio_mysql
    print(f"registros recuperados mysql: {registros_mysql}")
    print(f"tiempo obtenido en mostrar el item con la media mas alta (x5 al menos 10 reviews) mysql: {tiempo_mysql:.6f}")

    pipeline = [
        {"$group": {"_id": "$asin", "media_notas": {"$avg": "$overall"}, "total_reviews": {"$sum": 1}}},
        {"$match": {"total_reviews": {"$gte": 10}}},
        {"$sort": {"media_notas": -1, "_id": 1}},
        {"$limit": 1}
    ]
    registros_mongo = 0
    inicio_mongo = time.time()
    for _ in range(5):
        resultados_mongo = list(coleccion_mongo.aggregate(pipeline))
        registros_mongo = len(resultados_mongo)
    fin_mongo = time.time()
    tiempo_mongo = fin_mongo - inicio_mongo
    print(f"registros recuperados mongodb: {registros_mongo}")
    print(f"tiempo obtenido en mostrar el item con la media mas alta (x5 al menos 10 reviews) mongodb: {tiempo_mongo:.6f}")

    return tiempo_mysql, tiempo_mongo


def Benchmark_6():
    print("\nbenchmark6: numero de reviews por nota")

    sql_query = "SELECT overall, COUNT(*) as total_reviews FROM reviews WHERE unixReviewTime >= 1000000000 GROUP BY overall ORDER BY total_reviews ASC"
    registros_mysql = 0
    inicio_mysql = time.time()
    for _ in range(5):
        cursor.execute(sql_query)
        resultados = cursor.fetchall()
        registros_mysql = len(resultados)
    fin_mysql = time.time()
    tiempo_mysql = fin_mysql - inicio_mysql
    print(f"registros recuperados mysql: {registros_mysql}")
    print(f"tiempo obtenido en mostrar el numero de reviews por cada nota sql (x5): {tiempo_mysql:.6f}")

    pipeline = [
        {"$match": {"unixReviewTime": {"$gte": 1000000000}}},
        {"$group": {"_id": "$overall", "total_reviews": {"$sum": 1}}},
        {"$sort": {"total_reviews": 1}}
    ]
    registros_mongo = 0
    inicio_mongo = time.time()
    for _ in range(5):
        resultados_mongo = list(coleccion_mongo.aggregate(pipeline))
        registros_mongo = len(resultados_mongo)
    fin_mongo = time.time()
    tiempo_mongo = fin_mongo - inicio_mongo
    print(f"registros recuperados mongodb: {registros_mongo}")
    print(f"tiempo obtenido en mostrar el numero de reviews por cada nota mongo (x5): {tiempo_mongo:.6f}")

    return tiempo_mysql, tiempo_mongo


def Benchmark_7():
    print("\nbenchmark7: texto de rpgs")

    sql_query = "SELECT id FROM reviews WHERE summary LIKE '%RPG%' OR reviewText LIKE '%RPG%'"
    inicio_mysql = time.time()
    cursor.execute(sql_query)
    resultados = cursor.fetchall()
    registros_mysql = len(resultados)
    fin_mysql = time.time()
    tiempo_mysql = fin_mysql - inicio_mysql
    print(f"registros recuperados mysql: {registros_mysql}")
    print(f"tiempo obtenido en mostrar el numero de rpgs mysql (x1): {tiempo_mysql:.6f}")

    query_mongo = {"$or": [{"summary": {"$regex": "RPG", "$options": "i"}}, {"reviewText": {"$regex": "RPG", "$options": "i"}}]}
    inicio_mongo = time.time()
    resultados_mongo = list(coleccion_mongo.find(query_mongo))
    registros_mongo = len(resultados_mongo)
    fin_mongo = time.time()
    tiempo_mongo = fin_mongo - inicio_mongo
    print(f"registros recuperados mongodb: {registros_mongo}")
    print(f"tiempo obtenido en mostrar el numero de rpgs mongodb (x1): {tiempo_mongo:.6f}")

    return tiempo_mysql, tiempo_mongo


def generar_grafica(tiempos_mysql, tiempos_mongo):
    """genera la grafica de barras comparativa."""
    print("\ngenerando grafica comparativa")
    etiquetas = ['Q1', 'Q2', 'Q3', 'Q4', 'Q5', 'Q6', 'Q7']
    x = np.arange(len(etiquetas))
    ancho_barra = 0.35
    
    fig, ax = plt.subplots(figsize=(10, 6))
    
    barras_mysql = ax.bar(x - ancho_barra/2, tiempos_mysql, ancho_barra, label='MySQL', color='#6c96c2')
    barras_mongo = ax.bar(x + ancho_barra/2, tiempos_mongo, ancho_barra, label='MongoDB', color='#00ff00')
    
    ax.set_ylabel('Tiempo (segundos)')
    ax.set_title('Benchmarking: MySQL vs MongoDB')
    ax.set_xticks(x)
    ax.set_xticklabels(etiquetas)
    ax.legend()
    ax.grid(axis='y', linestyle='--', alpha=0.7)
    
    plt.savefig('grafica_benchmarking.png')
    plt.show()

if __name__ == "__main__":
    print("iniciando torneo de benchmarking: mysql vs mongodb\n")
    
    t_mysql = []
    t_mongo = []
    
    t_sq, t_mg = Benchmark_1(); t_mysql.append(t_sq); t_mongo.append(t_mg)
    t_sq, t_mg = Benchmark_2(); t_mysql.append(t_sq); t_mongo.append(t_mg)
    t_sq, t_mg = Benchmark_3(); t_mysql.append(t_sq); t_mongo.append(t_mg)
    t_sq, t_mg = Benchmark_4(); t_mysql.append(t_sq); t_mongo.append(t_mg)
    t_sq, t_mg = Benchmark_5(); t_mysql.append(t_sq); t_mongo.append(t_mg)
    t_sq, t_mg = Benchmark_6(); t_mysql.append(t_sq); t_mongo.append(t_mg)
    t_sq, t_mg = Benchmark_7(); t_mysql.append(t_sq); t_mongo.append(t_mg)
    
    generar_grafica(t_mysql, t_mongo)
    
    print("\ntorneo y grafica completados")