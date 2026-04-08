queries = {
    "Muestra el título (solo título) de las películas que fueron estrenadas en 2006.": 
        
        """
        MATCH(peliculas:Movie {released: 2006})
        RETURN peliculas.title
        
        """,
    "Obtén las personas (con todos los datos, es decir los nodos), que han dirigido películas que se estrenaron en el año 2006. Muestra la persona y las películas.":
        
        """
        MATCH(personas:Person)-[relacion:DIRECTED]->(peliculas:Movie{released:2006})
        RETURN personas, peliculas
        
        """,
    "Obtén todas las películas (el título) en las que estuvo involucrado Clint Eastwood y muestra el tipo de relación":
    
    """
        MATCH(persona:Person{name: 'Clint Eastwood'})-[relacion]->(pelicula:Movie)
        RETURN pelicula.title, type(relacion)
        
    """,
    "Muestra el título de la película y el personaje que interpreta de todas las películas en las que actúa Keanu Reeves.":
        
        """
        MATCH(persona: Person {name: 'Keanu Reeves'})-[relacion:ACTED_IN]->(peliculas:Movie)
        RETURN peliculas.title, relacion.roles
        
        """,
    "Mostrar todas las relaciones con la etiqueta de REVIEWED en las que el resumen de la revisión contiene la cadena “but”, ya sea en minúsculas o mayúsculas. Muestra el nombre de la persona que hizo la review, el título de la película revisada, la calificación y el resumen de la review.":
        
        """
        MATCH(personas:Person)-[relacion:REVIEWED]->(peliculas:Movie)
        WHERE toLower(relacion.summary) CONTAINS 'but'
        RETURN personas.name, peliculas.title, relacion.rating, relacion.summary
        
        """,
    "Muestra todas las personas que han dirigido alguna película y que hayan actuado en otra que no sea la misma que han dirigido. Muestra el nodo de la persona y el de las películas en las que ha actuado pero las que no ha dirigido.":
        
        """
        MATCH(personas:Person)-[:DIRECTED]->(pelicula_dirigida:Movie)
        MATCH(personas)-[:ACTED_IN]->(pelicula_actuada:Movie)
        WHERE pelicula_dirigida<>pelicula_actuada
        return personas, pelicula_actuada
        
        """,
        
        "Muestra todas las personas que han producido una película, pero no la han dirigido ni han actuado en ella, devolviendo los nodos de las personas y las películas producidas.":
            
        """
        MATCH(personas:Person)-[:PRODUCED]->(pelicula_producida:Movie)
        WHERE NOT (personas)-[:ACTED_IN]->(pelicula_producida)
        AND NOT (personas)-[:DIRECTED]->(pelicula_producida)
        RETURN personas, pelicula_producida
        
        """,
        
        "Muestra las películas y sus actores donde uno de los actores también ha dirigido la película, devolviendo los nombres de los actores, el nombre del director (que también ha actuado) y el título de la película":
            
        """
        """,
        
        
    
}
