from neo4j import GraphDatabase

# ==========================================
# ⚙️ CONFIGURACIÓN (EDITA ESTO)
# ==========================================
URI = "bolt://localhost:7687"
USUARIO = "neo4j"
PASSWORD = "celeste20@.13" # <-- Pon aquí tu clave de Neo4J

RUTA_DEPARTAMENTOS = "email-Eu-core-department-labels.txt"
RUTA_CORREOS = "email-Eu-core1000.txt"
# ==========================================

def procesar_ficheros():
    nodos = []
    enlaces = []
    
    # Leer departamentos
    with open(RUTA_DEPARTAMENTOS, 'r') as f:
        for linea in f:
            datos = linea.strip().split()
            if len(datos) == 2:
                nodos.append({"id": int(datos[0]), "dept": int(datos[1])})
                
    # Leer correos
    with open(RUTA_CORREOS, 'r') as f:
        for linea in f:
            datos = linea.strip().split()
            if len(datos) == 2:
                enlaces.append({"sender": int(datos[0]), "receiver": int(datos[1])})
                
    return nodos, enlaces

def cargar_en_neo4j(tx, nodos, enlaces):
  
    print("borrando base de datos anterior.")
    tx.run("MATCH (n) DETACH DELETE n")
    
    
    print("cargando usuarios y departamentos.")
    tx.run("""
        UNWIND $nodos AS n
        CREATE (:User {id: n.id, department: n.dept})
    """, nodos=nodos)
    

    print("cargando relaciones de correos.")
    tx.run("""
        UNWIND $enlaces AS e
        MATCH (u1:User {id: e.sender})
        MATCH (u2:User {id: e.receiver})
        CREATE (u1)-[:WROTE_TO]->(u2)
    """, enlaces=enlaces)

if __name__ == "__main__":
    print("iniciando proceso de carga.")
    nodos, enlaces = procesar_ficheros()
    
    driver = GraphDatabase.driver(URI, auth=(USUARIO, PASSWORD))
    with driver.session() as session:
        session.execute_write(cargar_en_neo4j, nodos, enlaces)
        
    driver.close()
    print("carga terminada")