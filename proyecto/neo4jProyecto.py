import mysql.connector
import pandas as pd
from neo4j import GraphDatabase
import configuracion

class AppNeo4j:
    def __init__(self):
        self.driver = GraphDatabase.driver(
            configuracion.NEO4J_CONFIG["uri"], 
            auth=(configuracion.NEO4J_CONFIG["user"], configuracion.NEO4J_CONFIG["password"])
        )

    def close(self):
        self.driver.close()

    # --- APARTADO 4.1: Similitud entre usuarios ---
    def cargar_similitudes_usuarios(self, num_usuarios=30):
        print(f"Obteniendo los top {num_usuarios} usuarios y sus reviews desde MySQL...")
        conn_mysql = mysql.connector.connect(**configuracion.MYSQL_CONFIG)
        
        # 1. Obtenemos las reviews SOLO de los X usuarios con más reviews
        query = f"""
            SELECT reviewerID, asin, overall 
            FROM Reviews_Core 
            WHERE reviewerID IN (
                SELECT reviewerID FROM (
                    SELECT reviewerID, COUNT(*) as c 
                    FROM Reviews_Core 
                    GROUP BY reviewerID 
                    ORDER BY c DESC 
                    LIMIT {num_usuarios}
                ) as top_users
            )
        """
        df = pd.read_sql(query, conn_mysql)
        conn_mysql.close()

        if df.empty:
            print("No se encontraron datos.")
            return

        print("Calculando Correlacion de Pearson...")
        # 2. Pivotamos: Filas = asins, Columnas = Usuarios, Valores = Notas
        pivot_df = df.pivot_table(index='asin', columns='reviewerID', values='overall')
        
        # El método .corr() de Pandas aplica exactamente la fórmula de Pearson del PDF
        # y automáticamente ignora los artículos que no tienen en común.
        matriz_similitud = pivot_df.corr(method='pearson')

        print("Cargando nodos y enlaces en Neo4j...")
        with self.driver.session() as session:
            # Primero, limpiamos la base de datos de Neo4j para evitar basura de pruebas
            session.run("MATCH (n) DETACH DELETE n")

            # Creamos los nodos de los usuarios
            usuarios = df['reviewerID'].unique()
            for u in usuarios:
                session.run("MERGE (u:Usuario {reviewerID: $user_id})", user_id=u)

            # Creamos las relaciones basadas en la matriz de similitud
            # matriz_similitud.columns son los usuarios
            count_enlaces = 0
            for i in range(len(matriz_similitud.columns)):
                for j in range(i + 1, len(matriz_similitud.columns)):
                    user_u = matriz_similitud.columns[i]
                    user_v = matriz_similitud.columns[j]
                    similitud = matriz_similitud.iloc[i, j]

                    # Solo enlazamos si existe similitud (no es NaN)
                    if pd.notna(similitud):
                        session.run("""
                            MATCH (u:Usuario {reviewerID: $u_id})
                            MATCH (v:Usuario {reviewerID: $v_id})
                            MERGE (u)-[r:SIMILAR_A {pearson: $sim}]-(v)
                        """, u_id=user_u, v_id=user_v, sim=round(float(similitud), 4))
                        count_enlaces += 1

        print(f"Proceso terminado. Se crearon {len(usuarios)} nodos y {count_enlaces} enlaces en Neo4j.")

        # Consulta final requerida por el PDF: Mostrar el usuario con más vecinos
        with self.driver.session() as session:
            result = session.run("""
                MATCH (u:Usuario)-[r:SIMILAR_A]-(v:Usuario)
                RETURN u.reviewerID AS Usuario, COUNT(r) AS Vecinos
                ORDER BY Vecinos DESC LIMIT 1
            """)
            record = result.single()
            if record:
                print(f"El usuario con mas vecinos es {record['Usuario']} con {record['Vecinos']} enlaces.")
    
    # --- AÑADE ESTO DENTRO DE LA CLASE AppNeo4j ---
    
    # APARTADO 4.2: Enlaces entre usuarios y artículos aleatorios
    def cargar_enlaces_usuarios_articulos(self):
        cat = input("Introduce el tipo de artículo (ej. Video_Games, Toys_and_Games): ")
        
        try:
            n_articulos = int(input("Introduce el número de artículos aleatorios a seleccionar: "))
        except ValueError:
            print("Por favor, introduce un numero valido.")
            return

        print(f"Buscando {n_articulos} articulos aleatorios de '{cat}' en MySQL...")
        conn_mysql = mysql.connector.connect(**configuracion.MYSQL_CONFIG)
        cursor = conn_mysql.cursor()
        
        # 1. Seleccionamos N artículos aleatorios de esa categoría usando ORDER BY RAND()
        query_asins = f"""
            SELECT p.asin 
            FROM Productos p
            JOIN Categorias c ON p.id_categoria = c.id_categoria
            WHERE c.nombre_categoria = '{cat}'
            ORDER BY RAND() LIMIT {n_articulos}
        """
        cursor.execute(query_asins)
        asins_aleatorios = [row[0] for row in cursor.fetchall()]

        if not asins_aleatorios:
            print("No se encontraron articulos para esa categoria.")
            conn_mysql.close()
            return

        # 2. Obtenemos TODAS las reviews de esos artículos específicos
        format_strings = ','.join(['%s'] * len(asins_aleatorios))
        query_reviews = f"""
            SELECT reviewerID, asin, overall, unixReviewTime 
            FROM Reviews_Core 
            WHERE asin IN ({format_strings})
        """
        cursor.execute(query_reviews, tuple(asins_aleatorios))
        reviews = cursor.fetchall()
        conn_mysql.close()

        print(f"Cargando {len(reviews)} relaciones en Neo4j. Limpiando DB previa...")
        
        with self.driver.session() as session:
            # Limpiamos la base de datos como exige el PDF
            session.run("MATCH (n) DETACH DELETE n")

            # Insertamos nodos y relaciones
            for reviewerID, asin, nota, tiempo in reviews:
                session.run("""
                    MERGE (u:Usuario {reviewerID: $u_id})
                    MERGE (a:Articulo {asin: $a_id, categoria: $categoria})
                    MERGE (u)-[r:PUNTUO {nota: $nota, tiempo: $tiempo}]->(a)
                """, u_id=reviewerID, a_id=asin, categoria=cat, nota=nota, tiempo=tiempo)

        # Mensaje de finalización exigido por el PDF
        print("Carga finalizada. Ya puedes consultar los datos en Neo4j usando:")
        print("   MATCH (n) RETURN n")
    
    # --- APARTADO 4.3: Usuarios que ven distintos tipos de artículos ---
    def cargar_usuarios_multicategoria(self):
        print("Buscando los primeros 400 usuarios y filtrando multicategoria en MySQL...")
        conn_mysql = mysql.connector.connect(**configuracion.MYSQL_CONFIG)
        cursor = conn_mysql.cursor()

        # 1. Seleccionamos los primeros 400 usuarios por nombre
        query_top400 = """
            SELECT reviewerID, reviewerName
            FROM Usuarios
            ORDER BY reviewerName ASC
            LIMIT 400
        """
        cursor.execute(query_top400)
        top_400_ids = [row[0] for row in cursor.fetchall()]

        if not top_400_ids:
            print("No se encontraron usuarios.")
            conn_mysql.close()
            return

        # 2. De esos 400, buscamos cuántos artículos de cada categoría han consumido,
        # pero SOLO nos quedamos con los usuarios que tienen >= 2 categorías distintas.
        format_strings = ','.join(['%s'] * len(top_400_ids))
        query_multicategoria = f"""
            SELECT r.reviewerID, c.nombre_categoria, COUNT(*) as cantidad
            FROM Reviews_Core r
            JOIN Productos p ON r.asin = p.asin
            JOIN Categorias c ON p.id_categoria = c.id_categoria
            WHERE r.reviewerID IN ({format_strings})
            GROUP BY r.reviewerID, c.nombre_categoria
            HAVING r.reviewerID IN (
                -- Subconsulta para contar cuántas categorías distintas tiene el usuario
                SELECT reviewerID 
                FROM (
                    SELECT r2.reviewerID, COUNT(DISTINCT p2.id_categoria) as num_cats
                    FROM Reviews_Core r2
                    JOIN Productos p2 ON r2.asin = p2.asin
                    WHERE r2.reviewerID IN ({format_strings})
                    GROUP BY r2.reviewerID
                    HAVING num_cats > 1
                ) as multi
            )
        """
        cursor.execute(query_multicategoria, tuple(top_400_ids) + tuple(top_400_ids))
        resultados = cursor.fetchall()
        conn_mysql.close()

        if not resultados:
            print("Ninguno de los primeros 400 usuarios ha puntuado en mas de 1 categoria.")
            return

        print(f"Cargando {len(resultados)} relaciones en Neo4j. Limpiando DB previa...")
        
        with self.driver.session() as session:
            # Limpiar DB
            session.run("MATCH (n) DETACH DELETE n")

            for reviewerID, categoria, cantidad in resultados:
                session.run("""
                    MERGE (u:Usuario {reviewerID: $u_id})
                    MERGE (c:Categoria {nombre: $cat_nombre})
                    MERGE (u)-[r:CONSUMIO_TIPO {cantidad: $cant}]->(c)
                """, u_id=reviewerID, cat_nombre=categoria, cant=cantidad)

        print("Carga finalizada para el apartado 4.3.")
        print("   Consulta en Neo4j usando: MATCH (n) RETURN n")
        
    # --- APARTADO 4.4: Artículos populares (<40 reviews) y usuarios en común ---
    def cargar_articulos_populares_comun(self):
        print("Buscando los 5 articulos (con <40 reviews) mas populares en MySQL...")
        conn_mysql = mysql.connector.connect(**configuracion.MYSQL_CONFIG)
        cursor = conn_mysql.cursor()

        # 1. Obtenemos los 5 artículos
        query_top5 = """
            SELECT asin, COUNT(*) as num_reviews
            FROM Reviews_Core
            GROUP BY asin
            HAVING num_reviews < 40
            ORDER BY num_reviews DESC
            LIMIT 5
        """
        cursor.execute(query_top5)
        top_5_asins = [row[0] for row in cursor.fetchall()]

        if not top_5_asins:
            print("No se encontraron articulos que cumplan la condicion.")
            conn_mysql.close()
            return

        # 2. Obtenemos a todos los usuarios que reseñaron esos 5 artículos
        format_strings = ','.join(['%s'] * len(top_5_asins))
        query_reviews = f"""
            SELECT reviewerID, asin, overall
            FROM Reviews_Core
            WHERE asin IN ({format_strings})
        """
        cursor.execute(query_reviews, tuple(top_5_asins))
        reviews = cursor.fetchall()
        conn_mysql.close()

        print(f"Cargando {len(reviews)} reviews en Neo4j y calculando usuarios en comun...")
        
        with self.driver.session() as session:
            # Limpiar DB previa
            session.run("MATCH (n) DETACH DELETE n")

            # 3. Insertar Usuarios, Artículos y su relación PUNTUO
            for reviewerID, asin, nota in reviews:
                session.run("""
                    MERGE (u:Usuario {reviewerID: $u_id})
                    MERGE (a:Articulo {asin: $a_id})
                    MERGE (u)-[r:PUNTUO {nota: $nota}]->(a)
                """, u_id=reviewerID, a_id=asin, nota=nota)
                
            # 4. Magia Cypher: Buscar usuarios que puntuaron el MISMO artículo
            # y crear un enlace "EN_COMUN" entre ellos con la cantidad.
            session.run("""
                MATCH (u1:Usuario)-[:PUNTUO]->(a:Articulo)<-[:PUNTUO]-(u2:Usuario)
                WHERE u1.reviewerID < u2.reviewerID  // Condición para no crear líneas dobles (ida y vuelta)
                WITH u1, u2, COUNT(a) as articulos_en_comun
                MERGE (u1)-[r:EN_COMUN {cantidad: articulos_en_comun}]-(u2)
            """)

        # Mensajes de finalización requeridos por el PDF
        print("Carga finalizada en Neo4j para el apartado 4.4.")
        print("   Consulta en Neo4j usando: MATCH (n) RETURN n")

# --- AHORA ACTUALIZA TU FUNCIÓN MENU_NEO4J PARA INCLUIR LA OPCIÓN ---
def menu_neo4j():
    app = AppNeo4j()
    while True:
        print("\n" + "="*40)
        print("MENU NEO4J - ANALISIS DE GRAFOS")
        print("="*40)
        print("1. Similitudes entre usuarios (Apartado 4.1)")
        print("2. Enlaces usuarios-artículos aleatorios (Apartado 4.2)")
        print("3. Usuarios multicategoría (Apartado 4.3)")
        print("4. Artículos populares y usuarios en común (Apartado 4.4)")
        print("5. Salir")
        
        opcion = input("Selecciona una opcion: ").strip()
        
        if opcion == "1":
            app.cargar_similitudes_usuarios(30)
        elif opcion == "2":
            app.cargar_enlaces_usuarios_articulos()
        elif opcion == "3":
            app.cargar_usuarios_multicategoria()
        elif opcion == "4":
            app.cargar_articulos_populares_comun()
        elif opcion == "5":
            print("Saliendo de Neo4j...")
            app.close()
            break
        else:
            print("Opcion no valida.")

if __name__ == "__main__":
    menu_neo4j()