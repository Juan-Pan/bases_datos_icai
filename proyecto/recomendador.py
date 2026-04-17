import mysql.connector
import pandas as pd
import configuracion

def recomendar_articulos():
    print("SISTEMA DE RECOMENDACIÓN")
    usuario = input("Introduce el ID del usuario (reviewerID): ")
    categoria = input("Introduce la categoría (ej. Video_Games): ")
    
    conn = mysql.connector.connect(**configuracion.MYSQL_CONFIG)
    
    # LA LÓGICA SQL: Buscamos artículos populares de esa categoría, 
    # pero EXCLUIMOS (NOT IN) los artículos que este usuario ya reseñó.
    query = f"""
        SELECT r.asin, COUNT(*) as Popularidad, AVG(r.overall) as Nota_Media
        FROM Reviews_Core r
        JOIN Productos p ON r.asin = p.asin
        JOIN Categorias c ON c.id_categoria = p.id_categoria
        WHERE c.nombre_categoria = '{categoria}'
          AND r.asin NOT IN (
              SELECT asin FROM Reviews_Core WHERE reviewerID = '{usuario}'
          )
        GROUP BY r.asin
        ORDER BY Popularidad DESC, Nota_Media DESC
        LIMIT 10
    """
    
    try:
        df = pd.read_sql(query, conn)
        if df.empty:
            print("No hay recomendaciones disponibles (quizás la categoría no existe).")
        else:
            print("\nTOP 10 RECOMENDACIONES PARA EL USUARIO")
            print(df.to_string(index=False))
    except Exception as e:
        print(f"Error: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    recomendar_articulos()