# Metacritic Videogame Scores Study

![Scala](https://img.shields.io/badge/Scala-DC322F?style=for-the-badge&logo=scala&logoColor=white)
![Spark](https://img.shields.io/badge/Apache_Spark-FFFFFF?style=for-the-badge&logo=apachespark&logoColor=#E35A16)
![Jupyter](https://img.shields.io/badge/Jupyter-F37626.svg?&style=for-the-badge&logo=Jupyter&logoColor=white)

***Leer en Español [🇪🇸](README_ES.md)***

This TFG project is about an *end-to-end* Data Engineering project with Spark and Scala. The main goal is to design a scalable pipeline to process, transform, and analyze a massive dataset extracted from **Metacritic** (featuring over 130,000 video game records).

Through this case study, the project explores the duality of opinions in the gaming industry: **What do professional critics value compared to what actual players enjoy?**

Beyond the industry analysis, this project serves as a **data engineering testbed**, benchmarking the performance impact of different storage formats and distributed computing architectures in the cloud.

## Architecture & Data Pipeline

The data lifecycle in this project is structured into 4 main phases:

1. **Data Ingestion & Modeling:** Reading raw data, defining strict schemas to ensure data quality, and transforming formats (from CSV to Parquet) to optimize I/O operations.
2. **Distributed Processing:** Developing the core analytical logic using the Apache Spark *DataFrame* API. This includes data cleaning, complex aggregations, and transforming nested structures.
3. **Visualization:** Consuming the processed data via Jupyter Notebook and generating interactive charts with Plotly.
4. **Deployment & Benchmarking:** The code is designed to be executed and benchmarked across three different environments:
    * **Local Environment**.
    * **AWS EMR:** Managed distributed cluster.
    * **AWS Glue:** *Serverless* environment.

## Analytical Use Cases (Queries)

The Spark pipeline is programmed to extract meaningful market insights through 7 key analytical queries about the videogame industry:

### 1. Score evolution over time
The objective of this query is to analyze the historical trend of average scores by release year to understand macro-shifts in game reception. By executing this logic, it answers the question of how average critic and user scores have changed across the decades.

### 2. Highest-rated genres
The objective of this query is to identify preferred genres by isolating and analyzing individual categories, even when a single game belongs to multiple genres at once. This resolves the question of whether there are genres universally praised by both experts and players, or if there are stark discrepancies between them.

### 3. Developers with the highest average quality
The objective of this query is to evaluate the consistency and overall quality of different development studios' work across their entire catalog. This addresses the question of which studios maintain the best historical quality average in their game releases.

### 4. Reviews by release year
The objective of this query is to measure community engagement and activity volume over time, tracking the growth of the user review ecosystem. It answers the question of how community participation has evolved compared to professional critic reviews by release year.

### 5. Most underrated genres
The objective of this query is to find niches of positive discrepancy, highlighting games that offer high entertainment value to players despite receiving lower technical scores from critics. This resolves the question of which specific niches users systematically grant higher scores than the press.

### 6. User polarization by videogame
The objective of this query is to detect highly debated games by calculating the dispersion index, or variance, of user ratings. This answers the question of which video games are the most divisive or controversial among the general public.

### 7. Critic vs user discrepancy by publisher
The objective of this query is to identify the publishers with the largest absolute mathematical gap between press praise and public reception scores. This resolves the question of which companies have the widest perception gap between what critics praise and what users actually enjoy.

> **📈 View the Visual Results** > You can explore the interactive charts generated for these queries on the **[Project's GitHub Pages](https://syekeon.github.io/Metacritic-Videogame-Scores-Study/index.html)**.

## Technologies Used

* **Core Language:** Scala 2.12.18
* **Big Data Engine:** Apache Spark 3.5.1
* **Cloud Infrastructure:** Amazon Web Services (S3, EMR 7.3.0, Glue 5.0)
* **Visualization:** Jupyter Notebook (Almond Scala Kernel), Plotly for Scala
* **Build Tool:** sbt 1.9.7