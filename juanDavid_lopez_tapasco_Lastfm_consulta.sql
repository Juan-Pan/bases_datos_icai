
-- PRÁCTICA LASTFM - SEGUNDA PARTE: CONSULTAS

-- CONSULTA 1: Obtener la media de edad de los usuarios que han escuchado a "Coldplay".
SELECT AVG(u.edad) AS media_edad
FROM usuarios u
WHERE u.id_usuario IN (
    SELECT DISTINCT e.id_usuario
    FROM escuchas e
    INNER JOIN canciones c ON e.id_cancion = c.id_cancion
    INNER JOIN artistas a ON c.id_artista = a.id_artista
    WHERE a.nombre_artista = 'Coldplay'
);


-- CONSULTA 2: Obtener el número total de hombres que han escuchado a "Janet Jackson" o "The Clash".

SELECT COUNT(DISTINCT u.id_usuario) AS total_hombres
FROM usuarios u
INNER JOIN escuchas e ON u.id_usuario = e.id_usuario
INNER JOIN canciones c ON e.id_cancion = c.id_cancion
INNER JOIN artistas a ON c.id_artista = a.id_artista
WHERE u.genero = 'm' 
  AND (a.nombre_artista = 'Janet Jackson' OR a.nombre_artista = 'The Clash');


-- CONSULTA 3: Obtener el número total de usuarios que o bien son de España (Spain) o bien han escuchado a 'Red Hot Chili Peppers'.
SELECT COUNT(DISTINCT id_usuario) AS total_usuarios
FROM (
    SELECT id_usuario 
    FROM usuarios 
    WHERE pais = 'Spain'
    UNION
    SELECT e.id_usuario
    FROM escuchas e
    INNER JOIN canciones c ON e.id_cancion = c.id_cancion
    INNER JOIN artistas a ON c.id_artista = a.id_artista
    WHERE a.nombre_artista = 'Red Hot Chili Peppers'
) AS usuarios_combinados;


-- CONSULTA 4: Obtener el promedio de escuchas por usuario para aquellos que tienen entre 19 y 21 años (incluyendo a los que no han escuchado nada).
SELECT AVG(conteo_escuchas) AS promedio_escuchas
FROM (
    SELECT u.id_usuario, COUNT(e.id_escucha) AS conteo_escuchas
    FROM usuarios u
    LEFT JOIN escuchas e ON u.id_usuario = e.id_usuario
    WHERE u.edad BETWEEN 19 AND 21
    GROUP BY u.id_usuario
) AS escuchas_por_usuario;


-- CONSULTA 5: Obtener los usuarios cuyo número total de escuchas superan a la media del total de escuchas de los usuarios.
SELECT id_usuario, COUNT(id_escucha) AS total_escuchas
FROM escuchas
GROUP BY id_usuario
HAVING COUNT(id_escucha) > (
    SELECT COUNT(e.id_escucha) / COUNT(DISTINCT u.id_usuario)
    FROM usuarios u
    LEFT JOIN escuchas e ON u.id_usuario = e.id_usuario
);


-- CONSULTA 6: Número total de escuchas por país.
SELECT u.pais, COUNT(e.id_escucha) AS total_escuchas
FROM usuarios u
INNER JOIN escuchas e ON u.id_usuario = e.id_usuario
GROUP BY u.pais;


-- CONSULTA 7: Obtener las 15 canciones que tienen un mayor número de escuchas de usuarios distintos.
SELECT c.nombre_cancion, COUNT(DISTINCT e.id_usuario) AS usuarios_distintos
FROM canciones c
INNER JOIN escuchas e ON c.id_cancion = e.id_cancion
GROUP BY c.id_cancion, c.nombre_cancion
ORDER BY usuarios_distintos DESC
LIMIT 15;


-- CONSULTA 8: Obtener el porcentaje de escuchas del total que han realizado los usuarios cuya edad supera la media de edad (ignorando edad 0 o nula).
SELECT 
    (COUNT(e.id_escucha) * 100.0 / (SELECT COUNT(*) FROM escuchas)) AS porcentaje_escuchas
FROM escuchas e
INNER JOIN usuarios u ON e.id_usuario = u.id_usuario
WHERE u.edad > (
    SELECT AVG(edad) 
    FROM usuarios 
    WHERE edad IS NOT NULL AND edad > 0
);