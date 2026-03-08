Querys = {
    "Pregunta 1":
        """
SELECT avg(edad) as edad_promedio
FROM escuchas 
INNER JOIN usuarios on escuchas.id_usuario = usuarios.id_usuario
INNER JOIN canciones on escuchas.id_cancion = canciones.id_cancion
INNER JOIN artistas on artistas.id_artista = canciones.id_artista
WHERE nombre_artista = 'Coldplay'
""", 
"Pregunta 2":
    """
    SELECT *"""
}