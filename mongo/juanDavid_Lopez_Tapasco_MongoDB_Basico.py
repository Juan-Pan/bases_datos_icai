queries = {
    "Pregunta 1": """
        db.peliculas.find({}, {_id: 0, pais: 0, actores: 0, genero: 0})
    """,
    "Pregunta 2": """
        db.peliculas.find({pais: "USA"}, {_id: 0, titulo: 1, director: 1, duracion: 1, puntuacion: 1})
    """,
    "Pregunta 3": """
        db.peliculas.find({pais: 'USA'}, {titulo: 1, director: 1, duracion: 1, puntuacion: 1, _id: 0}).limit(4)
    """,
    "Pregunta 4": """
        db.peliculas.find({}, {_id: 0, actores: 1, genero: 1, puntuacion: 1, titulo: 1}).sort({puntuacion: -1})
    """,
    "Pregunta 5": """
        db.peliculas.find({}, {_id: 0, titulo: 1, actores: 1, genero: 1, puntuacion: 1}).sort({puntuacion: -1}).limit(1)
    """,
    "Pregunta 6": """
        db.peliculas.find(
            {$or: [{año: {$gt: 2015}}, {puntuacion: {$gt: 8}}]},
            {_id: 0, titulo: 1, año: 1, puntuacion: 1}
        ).sort({año: 1, puntuacion: -1})
    """,
    "Pregunta 7": """
        db.peliculas.find({$and: [{año: {$gte: 1970}}, {año: {$lte: 2000}}]}).sort({año: 1})
    """,
    "Pregunta 8": """
        db.peliculas.find({duracion: {$lt: 150}, puntuacion: {$gte: 8}}).sort({duracion: 1, puntuacion: -1})
    """,
    "Pregunta 9": """
        db.peliculas.find(
            {director: {$in: ['George Lucas', 'Quentin Tarantino', 'Peter Jackson']}},
            {_id: 0, titulo: 1, director: 1, año: 1, actores: 1}
        )
    """,
    "Pregunta 10": """
        db.peliculas.find({"actores.0": {$in: ['Brad Pitt', 'Johnny Depp']}}).sort({duracion: -1})
    """,
    "Pregunta 11": """
        db.peliculas.find({secuela: {$exists: true}, año: {$gt: 2010}}).sort({año: 1})
    """,
    "Pregunta 12": """
        db.peliculas.find({actores: 'Harrison Ford'}, {_id: 0, titulo: 1, director: 1, actores: 1})
    """,
    "Pregunta 13": """
        db.peliculas.updateMany({director: 'Peter Jackson'}, {$set: {director: 'Jackson, Peter'}})
    """,
    "Pregunta 14": """
        db.peliculas.updateMany({pais: 'USA'}, {$set: {pais: 'EEUU'}})
    """,
    "Pregunta 15": """
        db.peliculas.bulkWrite([
            {
                updateOne: {
                    filter: {titulo: '8 apellidos vascos'},
                    update: {$set: {secuela: '8 apellidos catalanes', puntuacion: 7}}
                }
            },
            {
                deleteOne: {
                    filter: {puntuacion: {$lte: 5.5}}
                }
            },
            {
                replaceOne: {
                    filter: {titulo: 'El club de la lucha'},
                    replacement: {
                        titulo: "El club de los poetas muertos",
                        pais: 'EEUU',
                        año: 1989,
                        genero: 'drama',
                        duracion: 128,
                        presupuesto: 16.4,
                        recaudacion: 235.86,
                        puntuacion: 7.6,
                        actores: ['Robin Williams', 'Robert Sean Leonard']
                    }
                }
            }
        ])
    """,
    "Pregunta 16": """
        db.peliculas.find({puntuacion: {$gt: 7}})

        db.peliculas.find({puntuacion: {$gt: 7}}).explain("executionStats")

        db.peliculas.createIndex({puntuacion: -1})

        db.peliculas.find({puntuacion: {$gt: 7}}).explain("executionStats")
        //Antes de crear el índice, MongoDB realizaba un 'COLLSCAN' (escaneo completo de la colección), mirando todos los documentos uno a uno. Al crear el índice, MongoDB pasa a usar un 'IXSCAN' (escaneo de índice), reduciendo el número de documentos examinados (totalDocsExamined) y bajando el tiempo de ejecución (executionTimeMillis), haciendo la consulta mucho más eficiente. */
    """,
    "Pregunta 17": """
        db.peliculas.find({presupuesto: {$gt: 100}})

        db.peliculas.find({presupuesto: {$gt: 100}}).explain("executionStats")

        db.peliculas.createIndex({presupuesto: 1})

        db.peliculas.find({presupuesto: {$gt: 100}}).explain("executionStats")

        /* pasamos de un escaneo secuencial a uno indexado. El motor de base de datos ya no necesita leer los documentos que no cumplen la condición, accediendo a los que tienen un presupuesto mayor a 100 mediante el árbol del índice. */
    """,
    "Pregunta 18": """
        db.peliculas.createIndex({genero: "text"})

        db.peliculas.find(
            {$text: {$search: "acción comedia"}},
            {score: {$meta: "textScore"}}
        ).sort({score: {$meta: "textScore"}})
    """,
    "Pregunta 19": """
        db.peliculas.aggregate([
            {$group: {
                _id: "$clasificacion",
                total_recaudado: {$sum: "$recaudacion"},
                media_duracion: {$avg: "$duracion"},
                menor_puntuacion: {$min: "$puntuacion"},
                mayor_presupuesto: {$max: "$presupuesto"}
            }},
            {$sort: {media_duracion: 1}}
        ])
    """,
    "Pregunta 20": """
        db.peliculas.aggregate([
            {$group: {
                _id: "$pais",
                suma_presupuestos: {$sum: "$presupuesto"},
                suma_recaudaciones: {$sum: "$recaudacion"},
                media_puntuaciones: {$avg: "$puntuacion"},
                ultimo_ano: {$max: "$año"},
                minima_duracion: {$min: "$duracion"}
            }},
            {$sort: {ultimo_ano: 1}}
        ])
    """,
    "Pregunta 21": """
        db.peliculas.aggregate([
            {$match: {
                $or: [{productora: {$regex: "Marvel"}}, {puntuacion: {$gt: 7}}]
            }},
            {$group: {
                _id: "$productora",
                ano_mas_antigua: {$min: "$año"},
                presupuesto_medio: {$avg: "$presupuesto"},
                puntuacion_media: {$avg: "$puntuacion"}
            }}
        ])
    """,
    "Pregunta 22": """
        db.peliculas.aggregate([
            {$match: {
                $or: [{presupuesto: {$lt: 100}}, {puntuacion: {$gt: 8}}]
            }},
            {$group: {
                _id: "$clasificacion",
                recaudacion_media: {$avg: "$recaudacion"},
                mayor_presupuesto: {$max: "$presupuesto"},
                peor_puntuacion: {$min: "$puntuacion"},
                puntuacion_media: {$avg: "$puntuacion"}
            }}
        ])
    """
}
