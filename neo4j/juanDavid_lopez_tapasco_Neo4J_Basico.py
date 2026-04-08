queries = {
    "Muestra el título (solo título) de las películas que fueron estrenadas en 2006.": 
        
        """
        MATCH(peliculas:Movie {released: 2006})
        RETURN peliculas.title
        
        """,
    "Obtén las personas (con todos los datos, es decir los nodos), que han dirigido películas que se estrenaron en el año 2006. Muestra la persona y las películas.":
        
        """
        
        """,
}
