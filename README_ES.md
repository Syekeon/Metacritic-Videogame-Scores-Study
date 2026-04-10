# Estudio de Puntuaciones de Videojuegos en Metacritic

![Scala](https://img.shields.io/badge/Scala-DC322F?style=for-the-badge&logo=scala&logoColor=white)
![Spark](https://img.shields.io/badge/Apache_Spark-FFFFFF?style=for-the-badge&logo=apachespark&logoColor=#E35A16)
![Jupyter](https://img.shields.io/badge/Jupyter-F37626.svg?&style=for-the-badge&logo=Jupyter&logoColor=white)

***Read in English [🇬🇧](README.md)***

Este proyecto de TFG consiste en un proyecto *end-to-end* de Ingeniería de Datos con Spark y Scala. El objetivo principal es diseñar un pipeline escalable para procesar, transformar y analizar un conjunto de datos masivo extraído de **Metacritic** (con más de 130.000 registros de videojuegos).

A través de este caso de estudio, el proyecto explora la dualidad de opiniones en la industria de los videojuegos: **¿Qué valora la crítica profesional en comparación con lo que realmente disfrutan los jugadores?**

Más allá del análisis de la industria, este proyecto sirve como un **banco de pruebas de ingeniería de datos**, evaluando el impacto en el rendimiento de diferentes formatos de almacenamiento y arquitecturas de computación distribuida en la nube.

## Arquitectura y Pipeline de Datos

El ciclo de vida del dato en este proyecto se estructura en 4 fases principales:

1. **Ingesta y Modelado de Datos:** Lectura de datos crudos, definición de esquemas estrictos para garantizar la calidad del dato y transformación de formatos (de CSV a Parquet) para optimizar las operaciones I/O.
2. **Procesamiento Distribuido:** Desarrollo de la lógica analítica central utilizando la API *DataFrame* de Apache Spark. Esto incluye limpieza de datos, agregaciones complejas y transformación de estructuras anidadas.
3. **Visualización:** Consumo de los datos procesados mediante Jupyter Notebooks (Kernel de Scala) y generación de gráficos interactivos con Plotly.
4. **Despliegue y Benchmarking Cloud:** El código está diseñado para ser ejecutado y evaluado en tres entornos diferentes:
    * **Entorno Local**.
    * **AWS EMR:** Clúster gestionado.
    * **AWS Glue:** Entorno *serverless*.

## Casos de Uso Analítico (Queries)

El pipeline de Spark está programado para extraer perspectivas significativas del mercado a través de 7 queries analíticas clave sobre la industria de los videojuegos:

### 1. Evolución de puntuaciones a lo largo del tiempo
El objetivo de esta query es analizar la tendencia histórica de las puntuaciones medias por año de lanzamiento para comprender los macrocambios en la recepción de los juegos. Al ejecutar esta lógica, responde a la pregunta de cómo han cambiado las puntuaciones medias de la crítica y de los usuarios a lo largo de las décadas.

### 2. Géneros mejor valorados
El objetivo de esta query es identificar los géneros preferidos aislando y analizando categorías individuales, incluso cuando un solo juego pertenece a múltiples géneros a la vez. Esto resuelve la pregunta de si existen géneros elogiados universalmente tanto por expertos como por jugadores, o si existen discrepancias marcadas entre ellos.

### 3. Desarrolladoras con mejor media de calidad
El objetivo de esta query es evaluar la consistencia y la calidad general del trabajo de los diferentes estudios de desarrollo en todo su catálogo. Esto responde a la pregunta de qué estudios mantienen la mejor media de calidad histórica en sus lanzamientos.

### 4. Reseñas por año de lanzamientos
El objetivo de esta query es medir el compromiso de la comunidad y el volumen de actividad a lo largo del tiempo, rastreando el crecimiento del ecosistema de reseñas de usuarios. Responde a la pregunta de cómo ha evolucionado la participación de la comunidad en comparación con las reseñas de la crítica profesional por año de lanzamiento.

### 5. Géneros más infravalorados
El objetivo de esta query es encontrar nichos de discrepancia positiva, destacando juegos que ofrecen un alto valor de entretenimiento a los jugadores a pesar de recibir puntuaciones técnicas más bajas por parte de la crítica. Esto resuelve la pregunta de en qué nichos específicos los usuarios otorgan sistemáticamente puntuaciones más altas que la prensa.

### 6. Polarización de usuarios por videojuego
El objetivo de esta query es detectar juegos muy debatidos calculando el índice de dispersión, o varianza, de las valoraciones de los usuarios. Esto responde a la pregunta de cuáles son los videojuegos más divisivos o controversiales entre el público general.

### 7. Discrepancia crítica vs usuario por editora
El objetivo de esta query es identificar a las editoras (*publishers*) con la mayor brecha matemática absoluta entre los elogios de la prensa y las puntuaciones de recepción del público. Esto resuelve la pregunta de qué compañías tienen la mayor diferencia de percepción entre lo que alaba la crítica y lo que realmente disfrutan los usuarios.

> **📈 Mira los Resultados Visuales** > Puedes explorar los gráficos interactivos generados para estas queries en la **[GitHub Pages del Proyecto](https://syekeon.github.io/Metacritic-Videogame-Scores-Study/index.html)**.

## Tecnologías Utilizadas

* **Lenguaje Principal:** Scala 2.12.18
* **Motor Big Data:** Apache Spark 3.5.1
* **Infraestructura Cloud:** Amazon Web Services (S3, EMR 7.3.0, Glue 5.0)
* **Visualización:** Jupyter Notebook (Kernel Almond Scala), Plotly para Scala
* **Herramienta de Construcción:** sbt 1.9.7