import mysql.connector
import csv
from datetime import datetime
import sys

# ==============================================================================
# ⚙️ ZONA DE CONFIGURACIÓN (EDITAR AQUÍ)
# ==============================================================================
MYSQL_HOST = "localhost"
MYSQL_USER = "root"
MYSQL_PASS = "admin"  # <--- Pon aquí tu contraseña de MySQL
NOMBRE_BD  = "Lastfm"

# Rutas de los ficheros (Usa r"" para evitar problemas en Windows)
# Asegúrate de que los nombres coinciden con los que tienes descargados
RUTA_FICHERO_USUARIOS = r"C:\Users\lopez\Downloads\lastfm-dataset-1K (1)\lastfm-dataset-1K\userid-profile.tsv"
RUTA_FICHERO_ESCUCHAS = r"C:\Users\lopez\Downloads\lastfm-dataset-1K (1)\lastfm-dataset-1K\userid-timestamp-artid-artname-traid-traname.tsv"

# Límite de escuchas a procesar (Según el enunciado: 1 millón)
LIMITE_ESCUCHAS = 1000000 
# ==============================================================================

def main():
    print("--- INICIANDO EL SCRIPT DE LASTFM ---")
    
    # 1. CONEXIÓN Y CREACIÓN DE LA BASE DE DATOS
    try:
        conexion = mysql.connector.connect(
            host=MYSQL_HOST,
            user=MYSQL_USER,
            password=MYSQL_PASS
        )
        cursor = conexion.cursor()
        
        # Borrar y crear BD limpia
        cursor.execute(f"DROP DATABASE IF EXISTS {NOMBRE_BD}")
        cursor.execute(f"CREATE DATABASE {NOMBRE_BD}")
        cursor.execute(f"USE {NOMBRE_BD}")
        print(f"Base de datos '{NOMBRE_BD}' creada y seleccionada.")
        
    except mysql.connector.Error as err:
        print(f"Error de conexión: {err}")
        sys.exit(1)

    # 2. CREACIÓN DE TABLAS (DISEÑO 3FN) [cite: 30, 46]
    # Creamos IDs propios INT NOT NULL como pide el enunciado [cite: 38]
    
    # Tabla Artistas (Padre)
    cursor.execute("""
        CREATE TABLE artistas (
            id_artista INT NOT NULL,
            id_lastfm_artista VARCHAR(255),
            nombre_artista VARCHAR(255),
            PRIMARY KEY (id_artista)
        )
    """)

    # Tabla Usuarios (Padre) - Fecha registro DATE [cite: 32]
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

    # Tabla Canciones (Hija de Artistas)
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

    # Tabla Escuchas (Hija de Usuarios y Canciones) - Fecha DATETIME [cite: 32]
    # Aquí usamos AUTO_INCREMENT para la PK de la escucha
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

    # 3. PROCESAMIENTO DE USUARIOS
    # Necesitamos guardar el mapa {id_lastfm: id_interno} para usarlo luego en las escuchas
    mapa_usuarios = procesar_y_cargar_usuarios(cursor, conexion)

    # 4. PROCESAMIENTO DE ESCUCHAS (ARTISTAS + CANCIONES + LISTENINGS)
    procesar_y_cargar_escuchas(cursor, conexion, mapa_usuarios)

    # Cierre final
    cursor.close()
    conexion.close()
    print("\n--- PROCESO TERMINADO CON ÉXITO ---")


def procesar_y_cargar_usuarios(cursor, conexion):
    """Lee el fichero de perfiles y carga la tabla usuarios."""
    print(f"\nProcesando fichero de usuarios: {RUTA_FICHERO_USUARIOS}...")
    
    lista_para_insertar = []
    mapa_usuarios = {} # Diccionario para memoria: {'user_001': 1} [cite: 39]
    contador_id = 1
    
    try:
        with open(RUTA_FICHERO_USUARIOS, mode='r', encoding='utf-8') as f:
            lector = csv.reader(f, delimiter='\t') # [cite: 26]
            next(lector, None) # Saltar cabecera si existe

            for fila in lector:
                if len(fila) < 5: continue
                
                user_id_fm, genero, edad, pais, fecha_txt = fila[0], fila[1], fila[2], fila[3], fila[4]

                try:
                    edad_sql = int(edad) if edad and edad.strip() else None
                except ValueError:
                    edad_sql = None
                
                # Conversión de fecha: "Feb 18, 2026" -> "2026-02-18" [cite: 20, 33]
                try:
                    fecha_obj = datetime.strptime(fecha_txt, '%b %d, %Y')
                    fecha_sql = fecha_obj.strftime('%Y-%m-%d')
                except ValueError:
                    fecha_sql = None 

                if user_id_fm not in mapa_usuarios:
                    mapa_usuarios[user_id_fm] = contador_id
                    # Preparamos la tupla para insertar
                    lista_para_insertar.append((contador_id, user_id_fm, genero, edad_sql, pais, fecha_sql))
                    contador_id += 1
        
        # Inserción masiva (Mucho más rápido que insertar uno a uno)
        sql = "INSERT INTO usuarios (id_usuario, id_lastfm_usuario, genero, edad, pais, fecha_registro) VALUES (%s, %s, %s, %s, %s, %s)"
        cursor.executemany(sql, lista_para_insertar)
        conexion.commit()
        print(f" -> Insertados {len(lista_para_insertar)} usuarios.")
        return mapa_usuarios

    except FileNotFoundError:
        print(f"ERROR CRÍTICO: No se encuentra el fichero {RUTA_FICHERO_USUARIOS}")
        sys.exit(1)


def procesar_y_cargar_escuchas(cursor, conexion, mapa_usuarios):
    """
    Lee el fichero grande.
    1. Detecta Artistas y Canciones nuevos -> Los guarda en memoria.
    2. Guarda la Escucha vinculada a los IDs internos.
    3. Al final, hace los INSERTs masivos.
    """
    print(f"\nProcesando fichero de escuchas (Límite: {LIMITE_ESCUCHAS})...")
    
    # Listas para INSERT masivo al final
    datos_artistas = []
    datos_canciones = []
    datos_escuchas = []

    # Diccionarios para evitar duplicados y gestionar IDs [cite: 40]
    mapa_artistas = {}   # {'id_lastfm_artista': id_interno}
    mapa_canciones = {}  # {'id_lastfm_cancion': id_interno}
    
    cont_artista = 1
    cont_cancion = 1
    escuchas_procesadas = 0
    
    try:
        with open(RUTA_FICHERO_ESCUCHAS, mode='r', encoding='utf-8') as f:
            lector = csv.reader(f, delimiter='\t')
            # OJO: Este fichero a veces no tiene cabecera o es la primera línea.
            # Si ves que la primera línea no son datos, descomenta la siguiente línea:
            # next(lector, None) 

            for fila in lector:
                # Condición de parada: 1 millón de escuchas válidas [cite: 34]
                if escuchas_procesadas >= LIMITE_ESCUCHAS:
                    break
                
                if len(fila) < 6: continue # Saltar filas corruptas

                # Extraer datos: userid, timestamp, artid, artname, traid, traname
                user_fm  = fila[0]
                time_fm  = fila[1]
                art_id   = fila[2]
                art_name = fila[3]
                tra_id   = fila[4]
                tra_name = fila[5].strip() # Quitamos posibles \r\n del final [cite: 43]

                # 1. Filtro: Si no tiene ID de canción, saltamos [cite: 35]
                if not tra_id:
                    continue

                # 2. Gestión de ARTISTAS
                if art_id not in mapa_artistas:
                    mapa_artistas[art_id] = cont_artista
                    datos_artistas.append((cont_artista, art_id, art_name))
                    cont_artista += 1
                
                id_artista_interno = mapa_artistas[art_id]

                # 3. Gestión de CANCIONES
                if tra_id not in mapa_canciones:
                    mapa_canciones[tra_id] = cont_cancion
                    # La canción necesita saber quién es su padre (id_artista_interno)
                    datos_canciones.append((cont_cancion, tra_id, tra_name, id_artista_interno))
                    cont_cancion += 1
                
                id_cancion_interno = mapa_canciones[tra_id]

                # 4. Gestión de la ESCUCHA
                # Buscamos el ID interno del usuario que cargamos antes
                if user_fm in mapa_usuarios:
                    id_usuario_interno = mapa_usuarios[user_fm]
                    
                    # Convertir fecha: "2009-05-04T13:54:10Z" -> DATETIME SQL
                    # Formato del fichero: %Y-%m-%dT%H:%M:%SZ
                    try:
                        dt_obj = datetime.strptime(time_fm, '%Y-%m-%dT%H:%M:%SZ')
                        fecha_sql = dt_obj.strftime('%Y-%m-%d %H:%M:%S')
                    except ValueError:
                        fecha_sql = None # Si falla, null
                    
                    # Guardamos la escucha (user_interno, cancion_interno, fecha)
                    datos_escuchas.append((id_usuario_interno, id_cancion_interno, fecha_sql))
                    escuchas_procesadas += 1
                    
                    if escuchas_procesadas % 100000 == 0:
                        print(f" ... leídas {escuchas_procesadas} líneas válidas")

        print("Lectura finalizada. Volcando datos a MySQL (esto puede tardar un poco)...")

        # 5. INSERCIONES MASIVAS (Orden IMPORTANTE: Artistas -> Canciones -> Escuchas)
        
        # A) Artistas
        cursor.executemany("""INSERT INTO artistas (id_artista, id_lastfm_artista, nombre_artista) 
                              VALUES (%s, %s, %s)""", datos_artistas)
        print(f" -> Insertados {len(datos_artistas)} artistas.")

        # B) Canciones
        cursor.executemany("""INSERT INTO canciones (id_cancion, id_lastfm_cancion, nombre_cancion, id_artista) 
                              VALUES (%s, %s, %s, %s)""", datos_canciones)
        print(f" -> Insertadas {len(datos_canciones)} canciones.")

        # C) Escuchas
        cursor.executemany("""INSERT INTO escuchas (id_usuario, id_cancion, fecha_hora) 
                              VALUES (%s, %s, %s)""", datos_escuchas)
        print(f" -> Insertadas {len(datos_escuchas)} escuchas.")
        
        conexion.commit() # ¡Guardar todo!

    except FileNotFoundError:
        print(f"ERROR CRÍTICO: No se encuentra el fichero {RUTA_FICHERO_ESCUCHAS}")
        sys.exit(1)

if __name__ == "__main__":
    main()