import mysql.connector
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pymongo import MongoClient
from wordcloud import WordCloud
import configuracion

def obtener_conexion_sql():
    return mysql.connector.connect(**configuracion.MYSQL_CONFIG)

def obtener_conexion_mongo():
    client = MongoClient(configuracion.MONGO_CONFIG["uri"])
    db = client[configuracion.MONGO_CONFIG["database"]]
    return db[configuracion.MONGO_CONFIG["collection"]]

# 1. Evolución de reviews por años [cite: 75-77]
def grafico_reviews_por_año():
    conn = obtener_conexion_sql()
    cat = input("Introduce categoría (o 'todo'): ")
    
    query = """
        SELECT YEAR(fecha_formateada) as Año, COUNT(*) as Total
        FROM Reviews_Core r
        JOIN Productos p ON r.asin = p.asin
        JOIN Categorias c ON p.id_categoria = c.id_categoria
    """
    if cat.lower() != 'todo':
        categoria_limpia = cat.replace('_', ' ').lower()
        query += f" WHERE REPLACE(LOWER(c.nombre_categoria), '_', ' ') = '{categoria_limpia}'"
    query += " GROUP BY Año ORDER BY Año"
    
    df = pd.read_sql(query, conn)
    conn.close()

    plt.figure(figsize=(10,6))
    sns.barplot(data=df, x='Año', y='Total', color='steelblue')
    plt.title(f"Reviews por año - {cat.capitalize()}")
    plt.show()

# 2. Evolución de la popularidad de los artículos [cite: 90-92]
def grafico_popularidad_articulos():
    conn = obtener_conexion_sql()
    cat = input("Introduce categoría (o 'todo'): ")
    
    query = """
        SELECT r.asin, COUNT(*) as Numero_Reviews
        FROM Reviews_Core r
        JOIN Productos p ON r.asin = p.asin
        JOIN Categorias c ON p.id_categoria = c.id_categoria
    """
    if cat.lower() != 'todo':
        categoria_limpia = cat.replace('_', ' ').lower()
        query += f" WHERE REPLACE(LOWER(c.nombre_categoria), '_', ' ') = '{categoria_limpia}'"
    query += " GROUP BY r.asin ORDER BY Numero_Reviews DESC"
    
    df = pd.read_sql(query, conn)
    conn.close()

    plt.figure(figsize=(10,6))
    plt.plot(range(len(df)), df['Numero_Reviews'], color='teal')
    plt.title(f"Evolución de popularidad - {cat.capitalize()}")
    plt.xlabel("Artículos (ordenados por popularidad)")
    plt.ylabel("Número de reviews")
    plt.show()

# 3. Histograma por nota (y por artículo) [cite: 115-116, 127]
def grafico_histograma_notas():
    conn = obtener_conexion_sql()
    print("Opciones: 'todo', nombre de una 'categoria', o 'asin' de un artículo.")
    filtro = input("Introduce tu filtro: ")
    
    # Comprobamos si el filtro es un ASIN (longitud típica de Amazon de 10 caracteres alfanuméricos)
    if len(filtro) == 10 and filtro.isalnum() and filtro.lower() != 'todo':
        query = f"SELECT overall as Nota, COUNT(*) as Cantidad FROM Reviews_Core WHERE asin = '{filtro}' GROUP BY Nota ORDER BY Nota"
    else:
        query = """
            SELECT r.overall as Nota, COUNT(*) as Cantidad 
            FROM Reviews_Core r
            JOIN Productos p ON r.asin = p.asin
            JOIN Categorias c ON p.id_categoria = c.id_categoria
        """
        if filtro.lower() != 'todo':
            filtro_limpio = filtro.replace('_', ' ').lower()
            query += f" WHERE REPLACE(LOWER(c.nombre_categoria), '_', ' ') = '{filtro_limpio}'"
        query += " GROUP BY Nota ORDER BY Nota"
    
    df = pd.read_sql(query, conn)
    conn.close()

    if df.empty:
        print("Ese articulo/categoria no existe o no tiene reviews.")
        return

    plt.figure(figsize=(8,5))
    sns.barplot(data=df, x='Nota', y='Cantidad', color='#2b7bba')
    plt.title(f"Reviews por nota - {filtro.capitalize()}")
    plt.show()

# 4. Evolución de reviews a lo largo del tiempo (Suma Acumulativa) [cite: 136-138]
def grafico_evolucion_tiempo():
    conn = obtener_conexion_sql()
    # El PDF dice "por cada tipo de producto", haremos que el usuario elija
    cat = input("Introduce categoría: ")
    categoria_limpia = cat.replace('_', ' ').lower()
    query = f"""
        SELECT r.unixReviewTime
        FROM Reviews_Core r
        JOIN Productos p ON r.asin = p.asin
        JOIN Categorias c ON p.id_categoria = c.id_categoria
        WHERE REPLACE(LOWER(c.nombre_categoria), '_', ' ') = '{categoria_limpia}'
        ORDER BY r.unixReviewTime ASC
    """
    df = pd.read_sql(query, conn)
    conn.close()

    # Truco Pandas: Creamos un eje Y que sea simplemente 1, 2, 3, 4... (Suma acumulativa)
    df['Reviews_Acumuladas'] = range(1, len(df) + 1)

    plt.figure(figsize=(10,6))
    plt.plot(df['unixReviewTime'], df['Reviews_Acumuladas'], color='cadetblue')
    plt.title(f"Evolución de reviews a lo largo del tiempo - {cat.capitalize()}")
    plt.xlabel("Tiempo (Unix Timestamp)")
    plt.ylabel("Número de reviews hasta ese momento")
    plt.show()

# 5. Histograma de reviews por usuario [cite: 150-151]
def grafico_reviews_por_usuario():
    conn = obtener_conexion_sql()
    # Eje X = Número de reviews, Eje Y = Cuántos usuarios hicieron ese número
    query = """
        SELECT num_reviews as Numero_de_reviews, COUNT(*) as Numero_de_usuarios
        FROM (
            SELECT reviewerID, COUNT(*) as num_reviews
            FROM Reviews_Core
            GROUP BY reviewerID
        ) as subquery
        GROUP BY num_reviews
        ORDER BY Numero_de_reviews
    """
    df = pd.read_sql(query, conn)
    conn.close()

    plt.figure(figsize=(10,6))
    # Usamos barplot porque un histograma puro con plt.hist requeriría los datos sin agrupar
    sns.barplot(data=df, x='Numero_de_reviews', y='Numero_de_usuarios', color='#1f77b4')
    # Ocultamos las etiquetas de X si son muchas para que no se empaste
    if len(df) > 50: plt.xticks([]) 
    plt.title("Reviews por usuario")
    plt.xlabel("Número de reviews")
    plt.ylabel("Número de usuarios")
    plt.show()

# 6. Nube de palabras (Cruce MySQL + MongoDB) [cite: 174-176]
# 6. Nube de palabras (Cruce MySQL + MongoDB) - VERSIÓN CHUNKING
def nube_de_palabras():
    conn = obtener_conexion_sql()
    cat = input("Introduce la categoría para la nube de palabras: ")
    
    print("Buscando IDs en MySQL...")
    query = f"""
        SELECT r.id_review
        FROM Reviews_Core r
        JOIN Productos p ON r.asin = p.asin
        JOIN Categorias c ON p.id_categoria = c.id_categoria
        WHERE c.nombre_categoria = '{cat}'
    """
    cursor = conn.cursor()
    cursor.execute(query)
    ids_reviews = [row[0] for row in cursor.fetchall()]
    conn.close()

    if not ids_reviews:
        print("Categoria no encontrada.")
        return

    print(f"Extrayendo {len(ids_reviews)} resumenes de MongoDB (puede tardar un poco)...")
    col_mongo = obtener_conexion_mongo()
    
    # NUEVO: Procesamiento en lotes para evitar el límite de 16MB de BSON
    TAMANO_LOTE = 50000
    texto_completo = ""

    # Recorremos la lista de IDs en saltos de 50.000
    for i in range(0, len(ids_reviews), TAMANO_LOTE):
        lote_ids = ids_reviews[i:i + TAMANO_LOTE]
        
        # Pedimos solo los resúmenes de este lote
        documentos = col_mongo.find({"_id": {"$in": lote_ids}}, {"summary": 1, "_id": 0})
        
        # Concatenamos los textos del lote actual
        resumenes_lote = [doc.get('summary', '') for doc in documentos if isinstance(doc.get('summary'), str)]
        texto_completo += " ".join(resumenes_lote) + " "
        
        print(f"   -> Lote descargado ({min(i + TAMANO_LOTE, len(ids_reviews))}/{len(ids_reviews)})...")

    # Filtramos palabras con longitud mayor a 3 (según pide el PDF)
    palabras_filtradas = " ".join([word for word in texto_completo.split() if len(word) > 3])

    print("Generando nube de palabras...")
    wordcloud = WordCloud(width=800, height=400, background_color="white").generate(palabras_filtradas)

    plt.figure(figsize=(10,5))
    plt.imshow(wordcloud, interpolation='bilinear')
    plt.axis("off")
    plt.title(f"Nube de palabras (Summary) - {cat}")
    plt.show()

# 7. Gráfica Libre (Mi propuesta: Distribución de reviews por categoría) [cite: 185-186]
def grafico_libre():
    conn = obtener_conexion_sql()
    query = """
        SELECT c.nombre_categoria as Categoria, COUNT(r.id_review) as Total
        FROM Categorias c
        JOIN Productos p ON c.id_categoria = p.id_categoria
        JOIN Reviews_Core r ON p.asin = r.asin
        GROUP BY c.id_categoria
    """
    df = pd.read_sql(query, conn)
    conn.close()

    plt.figure(figsize=(8,8))
    plt.pie(df['Total'], labels=df['Categoria'], autopct='%1.1f%%', startangle=140, colors=sns.color_palette("pastel"))
    plt.title("Gráfico Libre: Porcentaje de Reviews por Categoría")
    plt.show()

def menu():
    conn = obtener_conexion_sql()
    cursor = conn.cursor()
    cursor.execute("SELECT nombre_categoria FROM Categorias")
    categorias_disp = [row[0].replace('_', ' ') for row in cursor.fetchall()]
    conn.close()

    while True: # [cite: 74]
        print("\n" + "="*40)
        print("MENU DE VISUALIZACION DE DATOS")
        print("="*40)
        print(f"Categorias disponibles: {', '.join(categorias_disp)}")
        print("-" * 40)
        print("1. Evolución de reviews por años")
        print("2. Evolución de popularidad de artículos")
        print("3. Histograma por nota (o por artículo)")
        print("4. Evolución temporal acumulada")
        print("5. Histograma de reviews por usuario")
        print("6. Nube de palabras (Summary)")
        print("7. Gráfica Libre (Distribución por Categorías)")
        print("8. Salir")
        
        opcion = input("Selecciona una opción (1-8): ")
        
        if opcion == "1": grafico_reviews_por_año()
        elif opcion == "2": grafico_popularidad_articulos()
        elif opcion == "3": grafico_histograma_notas()
        elif opcion == "4": grafico_evolucion_tiempo()
        elif opcion == "5": grafico_reviews_por_usuario()
        elif opcion == "6": nube_de_palabras()
        elif opcion == "7": grafico_libre()
        elif opcion == "8": 
            print("Saliendo del menu...")
            break # [cite: 187]
        else: print("Opcion no valida. Intentalo de nuevo.")

if __name__ == "__main__":
    menu()