import mysql.connector
import csv
from datetime import datetime
import sys

# configuración
MYSQL_HOST = "localhost"
MYSQL_USER = "root"
MYSQL_PASS = "admin"
NOMBRE_BD  = "Lastfm"

# rutas de los ficheros de entrada
RUTA_FICHERO_USUARIOS = r"C:\Users\lopez\Downloads\lastfm-dataset-1K (1)\lastfm-dataset-1K\userid-profile.tsv"
RUTA_FICHERO_ESCUCHAS = r"C:\Users\lopez\Downloads\lastfm-dataset-1K (1)\lastfm-dataset-1K\userid-timestamp-artid-artname-traid-traname.tsv"

# límite de escuchas a procesar
LIMITE_ESCUCHAS = 1000000 

def main():
    
    try:
        conexion = mysql.connector.connect(
            host=MYSQL_HOST,
            user=MYSQL_USER,
            password=MYSQL_PASS
        )
        cursor = conexion.cursor()
        
        # reiniciar base de datos
        cursor.execute(f"DROP DATABASE IF EXISTS {NOMBRE_BD}")
        cursor.execute(f"CREATE DATABASE {NOMBRE_BD}")
        cursor.execute(f"USE {NOMBRE_BD}")
        print(f"Base de datos '{NOMBRE_BD}' creada y seleccionada.")
        
    except mysql.connector.Error as err:
        print(f"Error de conexión: {err}")
        sys.exit(1)

    cursor.execute("""
        CREATE TABLE artistas (
            id_artista INT NOT NULL,
            id_lastfm_artista VARCHAR(255),
            nombre_artista VARCHAR(255),
            PRIMARY KEY (id_artista)
        )
    """)

    cursor.execute("""
        CREATE TABLE usuarios (
            id_usuario INT NOT NULL,
            id_lastfm_usuario VARCHAR(255),
            genero VARCHAR(50),
            edad INT,
            pais VARCHAR(255),
            fecha_registro DATE,
            PRIMARY KEY (id_usuario)
        )
    """)

    cursor.execute("""
        CREATE TABLE canciones (
            id_cancion INT NOT NULL,
            id_lastfm_cancion VARCHAR(255),
            nombre_cancion VARCHAR(255),
            id_artista INT,
            PRIMARY KEY (id_cancion),
            FOREIGN KEY (id_artista) REFERENCES artistas(id_artista)
        )
    """)

    # clave primaria autoincremental para escuchas
    cursor.execute("""
        CREATE TABLE escuchas (
            id_escucha INT NOT NULL AUTO_INCREMENT,
            id_usuario INT,
            id_cancion INT,
            fecha_hora DATETIME,
            PRIMARY KEY (id_escucha),
            FOREIGN KEY (id_usuario) REFERENCES usuarios(id_usuario),
            FOREIGN KEY (id_cancion) REFERENCES canciones(id_cancion)
        )
    """)
    print("Tablas creadas correctamente (Estructura 3FN).")

    # mapa de usuarios para enlazar escuchas
    mapa_usuarios = procesar_y_cargar_usuarios(cursor, conexion)

    procesar_y_cargar_escuchas(cursor, conexion, mapa_usuarios)

    cursor.close()
    conexion.close()
    print("\n--- PROCESO TERMINADO CON ÉXITO ---")


def procesar_y_cargar_usuarios(cursor, conexion):
    """lee el fichero de perfiles y carga la tabla usuarios."""
    print(f"\nProcesando fichero de usuarios: {RUTA_FICHERO_USUARIOS}...")
    
    lista_para_insertar = []
    mapa_usuarios = {}
    contador_id = 1
    
    try:
        with open(RUTA_FICHERO_USUARIOS, mode='r', encoding='utf-8') as f:
            lector = csv.reader(f, delimiter='\t')
            next(lector, None)

            for fila in lector:
                if len(fila) < 5: continue
                
                user_id_fm, genero, edad, pais, fecha_txt = fila[0], fila[1], fila[2], fila[3], fila[4]

                try:
                    edad_sql = int(edad) if edad and edad.strip() else None
                except ValueError:
                    edad_sql = None
                
                # convertir fecha a formato sql
                try:
                    fecha_obj = datetime.strptime(fecha_txt, '%b %d, %Y')
                    fecha_sql = fecha_obj.strftime('%Y-%m-%d')
                except ValueError:
                    fecha_sql = None 

                if user_id_fm not in mapa_usuarios:
                    mapa_usuarios[user_id_fm] = contador_id
                    lista_para_insertar.append((contador_id, user_id_fm, genero, edad_sql, pais, fecha_sql))
                    contador_id += 1
        
                # inserción masiva de usuarios
        sql = "INSERT INTO usuarios (id_usuario, id_lastfm_usuario, genero, edad, pais, fecha_registro) VALUES (%s, %s, %s, %s, %s, %s)"
        cursor.executemany(sql, lista_para_insertar)
        conexion.commit()
        print(f" -> Insertados {len(lista_para_insertar)} usuarios.")
        return mapa_usuarios

    except FileNotFoundError:
        print(f"ERROR CRÍTICO: No se encuentra el fichero {RUTA_FICHERO_USUARIOS}")
        sys.exit(1)


def procesar_y_cargar_escuchas(cursor, conexion, mapa_usuarios):
    """lee el fichero de escuchas y hace inserciones masivas."""
    print(f"\nProcesando fichero de escuchas (Límite: {LIMITE_ESCUCHAS})...")
    
    datos_artistas = []
    datos_canciones = []
    datos_escuchas = []

    mapa_artistas = {}
    mapa_canciones = {}
    
    cont_artista = 1
    cont_cancion = 1
    escuchas_procesadas = 0
    
    try:
        with open(RUTA_FICHERO_ESCUCHAS, mode='r', encoding='utf-8') as f:
            lector = csv.reader(f, delimiter='\t')

            for fila in lector:
                if escuchas_procesadas >= LIMITE_ESCUCHAS:
                    break
                
                if len(fila) < 6: continue

                user_fm  = fila[0]
                time_fm  = fila[1]
                art_id   = fila[2]
                art_name = fila[3]
                tra_id   = fila[4]
                tra_name = fila[5].strip()

                if not tra_id:
                    continue

                if art_id not in mapa_artistas:
                    mapa_artistas[art_id] = cont_artista
                    datos_artistas.append((cont_artista, art_id, art_name))
                    cont_artista += 1
                
                id_artista_interno = mapa_artistas[art_id]

                if tra_id not in mapa_canciones:
                    mapa_canciones[tra_id] = cont_cancion
                    datos_canciones.append((cont_cancion, tra_id, tra_name, id_artista_interno))
                    cont_cancion += 1
                
                id_cancion_interno = mapa_canciones[tra_id]

                if user_fm in mapa_usuarios:
                    id_usuario_interno = mapa_usuarios[user_fm]
                    
                    try:
                        dt_obj = datetime.strptime(time_fm, '%Y-%m-%dT%H:%M:%SZ')
                        fecha_sql = dt_obj.strftime('%Y-%m-%d %H:%M:%S')
                    except ValueError:
                        fecha_sql = None
                    
                    datos_escuchas.append((id_usuario_interno, id_cancion_interno, fecha_sql))
                    escuchas_procesadas += 1
                    
                    if escuchas_procesadas % 100000 == 0:
                        print(f" ... leídas {escuchas_procesadas} líneas válidas")

        print("Lectura finalizada. Volcando datos a MySQL (esto puede tardar un poco)...")

    # inserciones masivas en orden por claves foráneas
        cursor.executemany("""INSERT INTO artistas (id_artista, id_lastfm_artista, nombre_artista) 
                              VALUES (%s, %s, %s)""", datos_artistas)
        print(f" -> Insertados {len(datos_artistas)} artistas.")

        cursor.executemany("""INSERT INTO canciones (id_cancion, id_lastfm_cancion, nombre_cancion, id_artista) 
                              VALUES (%s, %s, %s, %s)""", datos_canciones)
        print(f" -> Insertadas {len(datos_canciones)} canciones.")

        cursor.executemany("""INSERT INTO escuchas (id_usuario, id_cancion, fecha_hora) 
                              VALUES (%s, %s, %s)""", datos_escuchas)
        print(f" -> Insertadas {len(datos_escuchas)} escuchas.")
        
        conexion.commit()

    except FileNotFoundError:
        print(f"ERROR CRÍTICO: No se encuentra el fichero {RUTA_FICHERO_ESCUCHAS}")
        sys.exit(1)

if __name__ == "__main__":
    main()