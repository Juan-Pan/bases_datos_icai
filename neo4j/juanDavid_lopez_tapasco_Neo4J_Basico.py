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
        
        "8. Muestra las películas y sus actores donde uno de los actores también ha dirigido la película, devolviendo los nombres de los actores, el nombre del director (que también ha actuado) y el título de la película.": 
        """
        
        MATCH (director_actor:Person)-[:DIRECTED]->(pelicula:Movie)
        MATCH (director_actor)-[:ACTED_IN]->(pelicula)
        MATCH (actores:Person)-[:ACTED_IN]->(pelicula)
        RETURN actores.name, director_actor.name, pelicula.title
        
        """,
    "9. Muestra todas las películas estrenadas en los años 2000, 2004, 2008, devolviendo sus títulos y años de estreno.": 
        """
        
        MATCH (pelicula:Movie)
        WHERE pelicula.released IN [2000, 2004, 2008]
        RETURN pelicula.title, pelicula.released
        
        """,
    "10. Mostrar todas la películas que han recibido al menos una puntuación (es decir, que tienen al menos una review) y que tienen director. Mostrar el grafo de la persona que la revisó, la película y su director.": 
        """
        
        MATCH (reviewer:Person)-[:REVIEWED]->(pelicula:Movie)<-[:DIRECTED]-(director:Person)
        RETURN reviewer, pelicula, director
        
        """,
    "11. Muestra los actores que hayan actuado en una película m con otro actor siempre y cuando el segundo actor haya dirigido una película donde aparecía el primer actor distinta a m. Retorna los nodos de los actores y de las películas involucradas.": 
        """
        
        MATCH (actor1:Person)-[:ACTED_IN]->(m:Movie)<-[:ACTED_IN]-(actor2:Person)
        MATCH (actor2)-[:DIRECTED]->(otra_peli:Movie)<-[:ACTED_IN]-(actor1)
        WHERE m <> otra_peli
        RETURN actor1, m, actor2, otra_peli
        
        """,
    "12. Muestra todas las películas en las que ha actuado Keanu Reeves, junto con los directores de las películas. Además, recupera los actores que actuaron en las mismas películas que Keanu Reeves. Devuelve el nombre de la película, el nombre del director y los nombres de los actores que trabajaron con Keanu Reeves.": 
        """
        
        MATCH (keanu:Person {name: 'Keanu Reeves'})-[:ACTED_IN]->(pelicula:Movie)
        MATCH (director:Person)-[:DIRECTED]->(pelicula)
        MATCH (otros_actores:Person)-[:ACTED_IN]->(pelicula)
        WHERE otros_actores <> keanu
        RETURN pelicula.title, director.name, otros_actores.name    
    
        """,
    "13. Muestra todas las películas en las que actuó Charlize Theron, devolviendo el título de la película, el año en que se estrenó, el número de años hace que se estrenó y la edad de Charlize Theron cuando se estrenó la película. Ordena la salida el número de años que hace que se estrenó.": 
        """
        
        MATCH (charlize:Person {name: 'Charlize Theron'})-[:ACTED_IN]->(pelicula:Movie)
        RETURN pelicula.title, pelicula.released, date().year - pelicula.released AS anos_pasados, pelicula.released - charlize.born AS edad_charlize
        ORDER BY anos_pasados
        
        """,
    "14. Muestra el nombre del actor y el número de películas en las que ha actuado siempre y cuando el actor haya actuado en al menos 5 películas.": 
        """
        
        MATCH (actor:Person)-[:ACTED_IN]->(pelicula:Movie)
        WITH actor, count(pelicula) AS num_peliculas
        WHERE num_peliculas >= 5
        RETURN actor.name, num_peliculas
        
        """,
    "15. Muestra los nombres de las personas que han hecho reseñas de películas y de los actores que aparecen en esas películas, devolviendo el nombre de la persona que ha hecho la reseña, el título de la película que ha reseñado, la fecha de estreno de la película, la calificación dada a la película por la persona que reseña y la lista de actores de esa película en particular (en formato lista).": 
        """
        
        MATCH (reviewer:Person)-[review:REVIEWED]->(pelicula:Movie)<-[:ACTED_IN]-(actor:Person)
        WITH reviewer, pelicula, review, collect(actor.name) AS lista_actores
        RETURN reviewer.name, pelicula.title, pelicula.released, review.rating, lista_actores
        
        """,
    "16. Muestra los nombres de los directores ordenados por orden alfabético mostrando también en forma de lista a todos los actores (sin repetición) que han actuado en sus películas.": 
        """
        
        MATCH (director:Person)-[:DIRECTED]->(pelicula:Movie)<-[:ACTED_IN]-(actor:Person)
        RETURN director.name, collect(DISTINCT actor.name) AS lista_actores
        ORDER BY director.name ASC
        
        """,
    "17. Muestra todas las películas junto con sus directores (los nodos) para aquellas películas que tengan al menos una review por una persona que tenga algún seguidor. Muestra también el nodo de la persona que ha revisado la película.": 
        """
        
        MATCH (director:Person)-[:DIRECTED]->(pelicula:Movie)<-[:REVIEWED]-(reviewer:Person)<-[:FOLLOWS]-(seguidor:Person)
        RETURN pelicula, director, reviewer
        
        """,
    "18. Muestra el grafo de las películas, junto con sus directores y los actores pero solo de aquellos directores que han dirigido mas de dos películas.": 
        """
        
        MATCH (director:Person)-[:DIRECTED]->(pelicula:Movie)
        WITH director, count(pelicula) AS num_pelis
        WHERE num_pelis > 2
        MATCH (director)-[:DIRECTED]->(p:Movie)<-[:ACTED_IN]-(a:Person)
        RETURN p, director, a
        
        """,
    "19.Modifica el papel que John Cusack interpretó en la película Stand By Me Denny Lachance a D. Lachance.": 
        """
        
        MATCH (john:Person {name: 'John Cusack'})-[rel:ACTED_IN]->(peli:Movie {title: 'Stand By Me'})
        SET rel.roles = ['D. Lachance']
        RETURN john, rel, peli
        
        """,
    "20. Crea un nuevo nodo con tu nombre y añade un rating y una review a una de las películasde tu elección.": 
        """
        
        MATCH (peli:Movie {title: 'The Matrix'})
        CREATE (yo:Person {name: 'Juan David'})
        CREATE (yo)-[:REVIEWED {rating: 100, summary: 'Obra maestra'}]->(peli)
        RETURN yo, peli
        
        """,

}
